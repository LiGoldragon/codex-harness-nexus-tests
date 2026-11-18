//! The Codex Harness Nexus as it is deployed: its pair launcher starts the
//! daemon and its translator in a scratch place whose runtime and state are
//! the test's own, and every exchange goes through the two CLIs,
//! `codex-harness` and `codex-harness-meta`, each given `<place-name>
//! '<datom>'`. The package is the one the `CODEX_HARNESS_NEXUS` variable
//! names; its `bin` holds `codex-harness-pair` and both CLIs.
//!
//! Every wait is on an event: a socket appearing, an answer, a process
//! ending. Each has a deadline, past which the test fails.

use std::fs;
use std::io::{BufRead as _, BufReader};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

use codex_harness_nexus::{CodexHarness, MemoryPlacing as _};
use flow::FlowId;
use harness::{Event, FilePath, Flow, Observed, State, Tokens};
use meta_signal_codex_harness_nexus as meta;
use nexus_exchange::SocketPaths;
use nix::sys::signal::{Signal as ProcessSignal, kill};
use nix::unistd::Pid;
use notify::{RecursiveMode, Watcher as _};
use shared_translator::{Place, PlaceName, Runtime, State as PlaceState};
use signal::Frame;
use signal_codex_harness_nexus::{
    Changed, Hook, Input, Observations, Output, ReportFailure, SessionStart_Data,
};
use translator_serving::{Budgeting as _, ValidTextBudget};

/// The variable naming the package the pair is started from.
const PACKAGE: &str = "CODEX_HARNESS_NEXUS";
/// The name of the scratch place, as the CLIs are given it.
const PLACE_NAME: &str = "alpha";
/// The longest any one event is waited for.
const DEADLINE: Duration = Duration::from_secs(60);

const SESSION: &str = "01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be";
const FLOW: &str = "4c85ce";
const SESSION_META: &str =
    "{\"type\":\"session_meta\",\"payload\":{\"id\":\"01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be\"}}\n";
const TASK_STARTED: &str = include_str!("../fixtures/task-started.jsonl");
const TOKEN_COUNT: &str = include_str!("../fixtures/token-count.jsonl");
const TASK_COMPLETE: &str = include_str!("../fixtures/task-complete-response-synthetic.jsonl");
/// The identity producer: it answers the one flow id only when it is asked
/// as the translator asks it, for this session.
const PRODUCER: &str = "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$2\" = --flows-root ] && [ \"$CODEX_SESSION_ID\" = 01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be ] || exit 2\necho 4c85ce\n";

/// An answer read back from a CLI's text.
trait Decoded: Sized {
    fn decoded(text: &str) -> Option<Self>;
}

impl Decoded for meta::Output {
    fn decoded(text: &str) -> Option<Self> {
        shared_translator::decode(text, &mut ValidTextBudget::MAXIMUM.budget()).ok()
    }
}

impl Decoded for Output {
    fn decoded(text: &str) -> Option<Self> {
        shared_translator::decode(text, &mut ValidTextBudget::MAXIMUM.budget()).ok()
    }
}

/// The test's own directory: the place's runtime and state, the private
/// home the processes see, the flows root, the sessions directory, the
/// identity producer, and a staging directory beside the sessions.
struct Scratch {
    directory: tempfile::TempDir,
}

trait Scratching: Sized {
    fn made() -> Self;
    fn path(&self, relative: &str) -> PathBuf;
    fn text(&self, relative: &str) -> String;
    fn place(&self) -> Place;
    /// The variables every process of the pair and every CLI is run with,
    /// so the place's name resolves inside the scratch directory.
    fn environment(&self) -> Vec<(&'static str, PathBuf)>;
    /// The Nexus's own settings, each valid, in the order they are set; the
    /// last makes the configuration ready.
    fn settings(&self) -> Vec<meta::Set>;
    /// Writes a rollout whole beside the sessions directory, then moves it
    /// in, so the kernel's word names it once it holds every row.
    fn placed(&self, name: &str, rows: &[&str]);
}

impl Scratching for Scratch {
    fn made() -> Self {
        let scratch = Self {
            directory: tempfile::tempdir().expect("scratch directory"),
        };
        let place = scratch.place();
        for directory in [
            place.runtime.0.as_str(),
            place.state.0.as_str(),
            &scratch.text("home"),
            &scratch.text("flows"),
            &scratch.text("sessions"),
            &scratch.text("staging"),
        ] {
            fs::create_dir_all(directory).expect("scratch subdirectory");
        }
        let producer = scratch.path("flow-id");
        fs::write(&producer, PRODUCER).expect("identity producer");
        fs::set_permissions(&producer, fs::Permissions::from_mode(0o755))
            .expect("identity producer is executable");
        scratch
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.directory.path().join(relative)
    }

    fn text(&self, relative: &str) -> String {
        self.path(relative).to_string_lossy().into_owned()
    }

    fn place(&self) -> Place {
        Place {
            place_name: PlaceName(PLACE_NAME.into()),
            runtime: Runtime(self.text(&format!("run/nexus/{PLACE_NAME}"))),
            state: PlaceState(self.text(&format!("state/nexus/{PLACE_NAME}"))),
        }
    }

    fn environment(&self) -> Vec<(&'static str, PathBuf)> {
        vec![
            ("XDG_RUNTIME_DIR", self.path("run")),
            ("XDG_STATE_HOME", self.path("state")),
            ("HOME", self.path("home")),
        ]
    }

    fn settings(&self) -> Vec<meta::Set> {
        vec![
            meta::Set::FlowsRoot(meta::FlowsRoot(self.text("flows"))),
            meta::Set::IdentityProducer(meta::IdentityProducer(self.text("flow-id"))),
            meta::Set::SessionsDirectories(meta::SessionsDirectories(vec![FilePath(
                self.text("sessions"),
            )])),
            meta::Set::ThreadDatabases(meta::ThreadDatabases(vec![])),
            meta::Set::SubscriberCapacity(meta::Capacity(16)),
        ]
    }

    fn placed(&self, name: &str, rows: &[&str]) {
        let staged = self.path("staging").join(name);
        fs::write(&staged, rows.concat()).expect("staged rollout");
        fs::rename(&staged, self.path("sessions").join(name)).expect("rollout moved in");
    }
}

/// The pair as its launcher runs it: the launcher, the daemon and the
/// translator it started, and the scratch they run in.
struct Pair {
    scratch: Scratch,
    launcher: Child,
    nexus: Pid,
    translator: Pid,
    ended: bool,
}

/// One of the pair's two processes, to end it by.
#[derive(Clone, Copy, Debug)]
enum Member {
    Nexus,
    Translator,
}

trait Pairing: Sized {
    /// Starts the pair and waits for the daemon's meta socket and both of
    /// the translator's sockets to appear.
    fn started() -> Self;
    fn sockets(&self) -> SocketPaths<CodexHarness>;
    fn executable(name: &str) -> PathBuf;
    fn meta(&self, input: meta::Input) -> meta::Output;
    /// A public request answered once, or the CLI's error when it is not
    /// answered.
    fn ask(&self, input: Input) -> Result<Output, String>;
    fn subscribe(&self) -> Subscription;
    /// The configuration as the meta socket reads it out.
    fn readout(&self) -> meta::ConfigurationReadout;
    /// Sets each of the Nexus's settings, checking each answer.
    fn configured(&self);
    /// Ends one member and waits for the launcher: it ends the other and
    /// fails, and neither process is left.
    fn end(self, member: Member);
}

impl Pairing for Pair {
    fn started() -> Self {
        let scratch = Scratch::made();
        let place = scratch.place();
        let sockets = SocketPaths::<CodexHarness>::from(&place);
        let (appeared, appearances) = channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = appeared.send(event);
        })
        .expect("a watch on the runtime directory");
        watcher
            .watch(Path::new(&place.runtime.0), RecursiveMode::NonRecursive)
            .expect("the runtime directory is watched");
        let launcher = Command::new(Self::executable("codex-harness-pair"))
            .arg(shared_translator::encode(&place))
            .envs(scratch.environment())
            .stdin(Stdio::null())
            .spawn()
            .expect("the pair launcher starts");
        for socket in [&sockets.meta, &sockets.translator, &sockets.meta_translator] {
            while !socket.exists() {
                appearances
                    .recv_timeout(DEADLINE)
                    .unwrap_or_else(|_| panic!("{} appears", socket.display()))
                    .expect("the runtime directory's watch reports");
            }
        }
        let children = fs::read_to_string(format!("/proc/{0}/task/{0}/children", launcher.id()))
            .expect("the launcher's children");
        let mut nexus = None;
        let mut translator = None;
        for child in children.split_whitespace() {
            let pid = Pid::from_raw(child.parse().expect("a process id"));
            let executable = fs::read_link(format!("/proc/{child}/exe")).expect("its executable");
            match executable.file_name().and_then(|name| name.to_str()) {
                Some("codex-harness-nexus") => nexus = Some(pid),
                Some("codex-harness-translator") => translator = Some(pid),
                other => panic!("the launcher started {other:?}"),
            }
        }
        Self {
            scratch,
            launcher,
            nexus: nexus.expect("the launcher started the daemon"),
            translator: translator.expect("the launcher started the translator"),
            ended: false,
        }
    }

    fn sockets(&self) -> SocketPaths<CodexHarness> {
        SocketPaths::<CodexHarness>::from(&self.scratch.place())
    }

    fn executable(name: &str) -> PathBuf {
        let package = std::env::var_os(PACKAGE)
            .unwrap_or_else(|| panic!("{PACKAGE} names the Codex Harness Nexus package"));
        Path::new(&package).join("bin").join(name)
    }

    fn meta(&self, input: meta::Input) -> meta::Output {
        let output = Command::new(Self::executable("codex-harness-meta"))
            .arg(PLACE_NAME)
            .arg(shared_translator::encode(&input))
            .envs(self.scratch.environment())
            .output()
            .expect("the meta CLI runs");
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{input:?} is answered: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        meta::Output::decoded(&text).unwrap_or_else(|| panic!("a meta answer: {text}"))
    }

    fn ask(&self, input: Input) -> Result<Output, String> {
        let output = Command::new(Self::executable("codex-harness"))
            .arg(PLACE_NAME)
            .arg(shared_translator::encode(&input))
            .envs(self.scratch.environment())
            .output()
            .expect("the public CLI runs");
        let text = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        Ok(Output::decoded(&text).unwrap_or_else(|| panic!("a public answer: {text}")))
    }

    fn subscribe(&self) -> Subscription {
        let mut child = Command::new(Self::executable("codex-harness"))
            .arg(PLACE_NAME)
            .arg(shared_translator::encode(&Input::Subscribe))
            .envs(self.scratch.environment())
            .stdout(Stdio::piped())
            .spawn()
            .expect("the public CLI subscribes");
        let stdout = child.stdout.take().expect("the subscription's output");
        let (answered, answers) = channel();
        // An answer may span lines; the lines read since the last answer
        // are one answer once they decode as one.
        std::thread::spawn(move || {
            let mut text = String::new();
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                text.push_str(&line);
                text.push('\n');
                if let Some(answer) = Output::decoded(&text) {
                    text.clear();
                    if answered.send(answer).is_err() {
                        return;
                    }
                }
            }
        });
        Subscription { child, answers }
    }

    fn readout(&self) -> meta::ConfigurationReadout {
        match self.meta(meta::Input::Subscribe) {
            meta::Output::Subscribed(Frame::Opened(readout)) => readout,
            other => panic!("the configuration is read back: {other:?}"),
        }
    }

    fn configured(&self) {
        for set in self.scratch.settings() {
            assert_eq!(
                self.meta(meta::Input::Configure(set.clone())),
                meta::Output::Configured(set)
            );
        }
    }

    fn end(mut self, member: Member) {
        let pid = match member {
            Member::Nexus => self.nexus,
            Member::Translator => self.translator,
        };
        kill(pid, ProcessSignal::SIGTERM).expect("the member is signalled");
        let status = self.launcher.wait().expect("the launcher ends");
        self.ended = true;
        assert!(
            !status.success(),
            "the launcher fails when a member ends: {status:?}"
        );
        for (name, pid) in [("daemon", self.nexus), ("translator", self.translator)] {
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "the {name} is ended and reaped once {member:?} ended"
            );
        }
    }
}

/// Whatever a failed test left running is killed.
impl Drop for Pair {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let _ = kill(self.nexus, ProcessSignal::SIGKILL);
        let _ = kill(self.translator, ProcessSignal::SIGKILL);
        let _ = self.launcher.kill();
        let _ = self.launcher.wait();
    }
}

/// A public subscription through the CLI, and its answers as they come.
struct Subscription {
    child: Child,
    answers: Receiver<Output>,
}

trait Following {
    fn next(&self) -> Output;
}

impl Following for Subscription {
    fn next(&self) -> Output {
        self.answers
            .recv_timeout(DEADLINE)
            .expect("the subscription answers")
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn session_start() -> Input {
    session_start_at(None)
}

/// The session's start, naming the rollout it will write, if it names one.
fn session_start_at(rollout: Option<&Path>) -> Input {
    Input::Report(Hook::SessionStart(SessionStart_Data {
        session: harness::Session(SESSION.into()),
        file_path_option: rollout.map(|path| FilePath(path.to_string_lossy().into_owned())),
    }))
}

/// The session as it starts, before its rollout is read.
fn dry() -> Observed {
    Observed {
        flow_id: FlowId(FLOW.into()),
        state: State::Dry,
        tokens: Tokens(0),
        background_work_vector: vec![],
    }
}

/// Follows a subscription until the turn ends, and answers each transition.
trait Turning {
    fn turn(&self) -> Vec<(Event, State)>;
}

impl Turning for Subscription {
    fn turn(&self) -> Vec<(Event, State)> {
        let mut transitions = vec![];
        while !matches!(transitions.last(), Some((Event::TurnEnded, _))) {
            match self.next() {
                Output::Subscribed(Frame::Changed(Changed::Transition(transition))) => {
                    assert_eq!(transition.flow_id, FlowId(FLOW.into()));
                    transitions.push((transition.event, transition.state));
                }
                other => panic!("the subscriber follows each transition: {other:?}"),
            }
        }
        transitions
    }
}

/// A sessions directory made whole beside the scratch, holding one rollout
/// of a whole turn, then moved to `directory`, so it appears with its
/// rollout already inside.
fn made_with_rollout(scratch: &Scratch, directory: &Path, rollout: &str) {
    let staged = scratch.path("staging").join("sessions");
    fs::create_dir(&staged).expect("a staged sessions directory");
    fs::write(
        staged.join(rollout),
        [SESSION_META, TASK_STARTED, TOKEN_COUNT, TASK_COMPLETE].concat(),
    )
    .expect("a staged rollout");
    fs::rename(&staged, directory).expect("the sessions directory moved in");
}

#[test]
fn the_public_socket_opens_only_once_the_configuration_is_ready() {
    let pair = Pair::started();
    let sockets = pair.sockets();
    let settings = pair.scratch.settings();
    let (last, before) = settings.split_last().expect("settings");
    for set in before {
        assert!(!sockets.public.exists(), "not ready before {set:?}");
        assert_eq!(
            pair.meta(meta::Input::Configure(set.clone())),
            meta::Output::Configured(set.clone())
        );
    }
    assert!(!sockets.public.exists(), "not ready before {last:?}");
    // The daemon has answered, so it has opened its Memory.
    assert!(
        pair.scratch.place().memory_path::<CodexHarness>().exists(),
        "the Memory is named after the Nexus, in the place's state"
    );
    assert_eq!(
        pair.meta(meta::Input::Configure(last.clone())),
        meta::Output::Configured(last.clone())
    );
    assert!(
        sockets.public.exists(),
        "the public socket is open once the configuration is ready"
    );
    let readout = pair.readout();
    assert_eq!(
        readout.awaited_directories,
        meta::AwaitedDirectories(vec![]),
        "a sessions directory there is not awaited"
    );
    let configuration = readout.configuration;
    assert_eq!(
        configuration,
        meta::Configuration {
            // The text budget is the translator's; this readout does not
            // carry it yet (see the ignored test below).
            text_budget_option: configuration.text_budget_option.clone(),
            flows_root_option: Some(meta::FlowsRoot(pair.scratch.text("flows"))),
            identity_producer_option: Some(meta::IdentityProducer(pair.scratch.text("flow-id"))),
            sessions_directories_option: Some(meta::SessionsDirectories(vec![FilePath(
                pair.scratch.text("sessions")
            )])),
            thread_databases_option: Some(meta::ThreadDatabases(vec![])),
            capacity_option: Some(meta::Capacity(16)),
        }
    );
    pair.end(Member::Nexus);
}

#[test]
fn an_invalid_setting_is_refused_typed_and_never_kept() {
    let pair = Pair::started();
    let relative = FilePath("relative/sessions".into());
    for (set, refusal) in [
        (
            meta::Set::SubscriberCapacity(meta::Capacity(0)),
            meta::ConfigurationRefusal::InvalidCapacity(meta::Capacity(0)),
        ),
        (
            meta::Set::SubscriberCapacity(meta::Capacity(-1)),
            meta::ConfigurationRefusal::InvalidCapacity(meta::Capacity(-1)),
        ),
        (
            meta::Set::FlowsRoot(meta::FlowsRoot("relative/flows".into())),
            meta::ConfigurationRefusal::InvalidPath(FilePath("relative/flows".into())),
        ),
        (
            meta::Set::SessionsDirectories(meta::SessionsDirectories(vec![
                FilePath(pair.scratch.text("sessions")),
                relative.clone(),
            ])),
            meta::ConfigurationRefusal::InvalidPath(relative),
        ),
        (
            meta::Set::TextBudget(meta::TextBudget {
                node_limit: meta::NodeLimit(0),
                depth_limit: meta::DepthLimit(32),
            }),
            meta::ConfigurationRefusal::InvalidTextBudget(meta::TextBudget {
                node_limit: meta::NodeLimit(0),
                depth_limit: meta::DepthLimit(32),
            }),
        ),
    ] {
        assert_eq!(
            pair.meta(meta::Input::Configure(set)),
            meta::Output::ConfigureRefused(refusal)
        );
    }
    let configuration = pair.readout().configuration;
    assert_eq!(
        configuration,
        meta::Configuration {
            text_budget_option: configuration.text_budget_option.clone(),
            flows_root_option: None,
            identity_producer_option: None,
            sessions_directories_option: None,
            thread_databases_option: None,
            capacity_option: None,
        },
        "no refused value is kept"
    );
    assert!(!pair.sockets().public.exists());
    pair.end(Member::Nexus);
}

#[test]
fn the_text_budget_is_answered_by_the_translator_and_bounds_its_reading() {
    let pair = Pair::started();
    let tight = meta::TextBudget {
        node_limit: meta::NodeLimit(2),
        depth_limit: meta::DepthLimit(2),
    };
    // Set before the Nexus is configured: the Nexus refuses a text budget
    // as TranslatorSetting, so this answer is the translator's own.
    assert_eq!(
        pair.meta(meta::Input::Configure(meta::Set::TextBudget(tight.clone()))),
        meta::Output::Configured(meta::Set::TextBudget(tight))
    );
    pair.configured();
    let refused = pair
        .ask(session_start())
        .expect_err("a request beyond the budget is not read");
    assert!(
        refused.contains("Budget"),
        "the translator refuses it for its budget: {refused}"
    );
    let roomy = meta::TextBudget {
        node_limit: meta::NodeLimit(1024),
        depth_limit: meta::DepthLimit(32),
    };
    assert_eq!(
        pair.meta(meta::Input::Configure(meta::Set::TextBudget(roomy.clone()))),
        meta::Output::Configured(meta::Set::TextBudget(roomy))
    );
    assert_eq!(pair.ask(session_start()), Ok(Output::Reported));
    pair.end(Member::Nexus);
}

#[test]
#[ignore = "translator-serving 0.1.1's meta_answered has no &self, so a forwarded Configuration readout carries the text budget as None until the shared release fixes it"]
fn the_configuration_readout_shows_the_translators_text_budget() {
    let pair = Pair::started();
    let budget = meta::TextBudget {
        node_limit: meta::NodeLimit(1024),
        depth_limit: meta::DepthLimit(32),
    };
    assert_eq!(
        pair.meta(meta::Input::Configure(meta::Set::TextBudget(
            budget.clone()
        ))),
        meta::Output::Configured(meta::Set::TextBudget(budget.clone()))
    );
    assert_eq!(
        pair.readout().configuration.text_budget_option,
        Some(budget)
    );
    pair.end(Member::Nexus);
}

#[test]
fn a_rollout_in_a_sessions_directory_is_discovered_folded_and_shown() {
    let pair = Pair::started();
    pair.configured();
    assert_eq!(
        pair.ask(Input::Report(Hook::PostToolUse(harness::Session(
            SESSION.into()
        )))),
        Ok(Output::ReportRefused(ReportFailure::Unregistered(
            harness::Session(SESSION.into())
        ))),
        "a hook before its session started names no session"
    );
    assert_eq!(pair.ask(session_start()), Ok(Output::Reported));
    let subscription = pair.subscribe();
    assert_eq!(
        subscription.next(),
        Output::Subscribed(Frame::Opened(Observations(vec![dry()])))
    );
    // A capacity set while a subscriber follows resizes its publisher.
    let resized = meta::Set::SubscriberCapacity(meta::Capacity(8));
    assert_eq!(
        pair.meta(meta::Input::Configure(resized.clone())),
        meta::Output::Configured(resized)
    );
    pair.scratch.placed(
        &format!("rollout-{SESSION}.jsonl"),
        &[SESSION_META, TASK_STARTED, TOKEN_COUNT, TASK_COMPLETE],
    );
    let transitions = subscription.turn();
    let [
        (Event::PromptSubmitted, State::Flowing(Flow::Working)),
        (Event::ContextSize(tokens), State::Flowing(Flow::Working)),
        (Event::Responded(_), _),
        (Event::TurnEnded, State::Flowing(Flow::Rested)),
    ] = &transitions[..]
    else {
        panic!("the rows tell the turn: {transitions:?}");
    };
    let shown = pair.subscribe();
    assert_eq!(
        shown.next(),
        Output::Subscribed(Frame::Opened(Observations(vec![Observed {
            flow_id: FlowId(FLOW.into()),
            state: State::Flowing(Flow::Rested),
            tokens: *tokens,
            background_work_vector: vec![],
        }]))),
        "a new subscriber is shown the folded session"
    );
    drop(shown);
    drop(subscription);
    pair.end(Member::Nexus);
}

#[test]
fn an_absent_sessions_directory_is_configured_awaited_and_found_once_it_is_made() {
    let pair = Pair::started();
    let sessions = pair.scratch.path("later/.codex/sessions");
    let directory = FilePath(sessions.to_string_lossy().into_owned());
    for set in pair.scratch.settings() {
        let set = match set {
            meta::Set::SessionsDirectories(_) => {
                meta::Set::SessionsDirectories(meta::SessionsDirectories(vec![directory.clone()]))
            }
            set => set,
        };
        assert_eq!(
            pair.meta(meta::Input::Configure(set.clone())),
            meta::Output::Configured(set),
            "a sessions directory not yet there is configured"
        );
    }
    assert!(
        pair.sockets().public.exists(),
        "the public socket does not wait for a sessions directory"
    );
    let readout = pair.readout();
    assert_eq!(
        readout.configuration.sessions_directories_option,
        Some(meta::SessionsDirectories(vec![directory.clone()])),
        "it is recorded"
    );
    assert_eq!(
        readout.awaited_directories,
        meta::AwaitedDirectories(vec![directory.clone()]),
        "it is awaited"
    );
    let name = format!("rollout-{SESSION}.jsonl");
    assert_eq!(
        pair.ask(session_start_at(Some(&sessions.join(&name)))),
        Ok(Output::Reported)
    );
    let subscription = pair.subscribe();
    assert_eq!(
        subscription.next(),
        Output::Subscribed(Frame::Opened(Observations(vec![dry()])))
    );
    fs::create_dir(pair.scratch.path("later")).expect("the first level");
    fs::create_dir(pair.scratch.path("later/.codex")).expect("the second level");
    made_with_rollout(&pair.scratch, &sessions, &name);
    let transitions = subscription.turn();
    assert_eq!(
        transitions.first(),
        Some(&(Event::PromptSubmitted, State::Flowing(Flow::Working))),
        "the rollout inside is read from its start: {transitions:?}"
    );
    assert_eq!(
        pair.readout().awaited_directories,
        meta::AwaitedDirectories(vec![]),
        "nothing is awaited once it is there"
    );
    drop(subscription);
    pair.end(Member::Nexus);
}

#[test]
fn a_sessions_directory_that_cannot_be_read_is_refused_typed_and_never_kept() {
    let pair = Pair::started();
    pair.configured();
    let locked = pair.scratch.path("locked");
    fs::create_dir(&locked).expect("a directory to lock");
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).expect("locked");
    assert!(
        fs::read_dir(&locked)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied),
        "this test runs as a user whom mode 000 denies"
    );
    let set = meta::Set::SessionsDirectories(meta::SessionsDirectories(vec![FilePath(
        pair.scratch.text("locked"),
    )]));
    let answer = pair.meta(meta::Input::Configure(set));
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).expect("unlocked");
    let meta::Output::ConfigureRefused(meta::ConfigurationRefusal::UnwatchableDirectory(
        unwatchable,
    )) = answer
    else {
        panic!("a directory that cannot be watched is refused typed: {answer:?}");
    };
    assert_eq!(unwatchable.file_path, FilePath(pair.scratch.text("locked")));
    assert!(
        unwatchable.diagnostic.0.contains("Permission denied"),
        "the refusal tells why: {}",
        unwatchable.diagnostic.0
    );
    let readout = pair.readout();
    assert_eq!(
        readout.configuration.sessions_directories_option,
        Some(meta::SessionsDirectories(vec![FilePath(
            pair.scratch.text("sessions")
        )])),
        "the refused directories are never kept"
    );
    assert_eq!(
        readout.awaited_directories,
        meta::AwaitedDirectories(vec![])
    );
    // The watch before the refusal still serves.
    let name = format!("rollout-{SESSION}.jsonl");
    assert_eq!(pair.ask(session_start()), Ok(Output::Reported));
    let subscription = pair.subscribe();
    assert_eq!(
        subscription.next(),
        Output::Subscribed(Frame::Opened(Observations(vec![dry()])))
    );
    pair.scratch.placed(
        &name,
        &[SESSION_META, TASK_STARTED, TOKEN_COUNT, TASK_COMPLETE],
    );
    subscription.turn();
    drop(subscription);
    pair.end(Member::Nexus);
}

#[test]
fn a_sessions_directory_removed_and_made_anew_is_watched_again() {
    let pair = Pair::started();
    pair.configured();
    let sessions = pair.scratch.path("sessions");
    let name = format!("rollout-{SESSION}.jsonl");
    assert_eq!(
        pair.ask(session_start_at(Some(&sessions.join(&name)))),
        Ok(Output::Reported)
    );
    let subscription = pair.subscribe();
    assert_eq!(
        subscription.next(),
        Output::Subscribed(Frame::Opened(Observations(vec![dry()])))
    );
    fs::remove_dir(&sessions).expect("the sessions directory removed");
    made_with_rollout(&pair.scratch, &sessions, &name);
    let transitions = subscription.turn();
    assert_eq!(
        transitions.first(),
        Some(&(Event::PromptSubmitted, State::Flowing(Flow::Working))),
        "the rollout in the directory made anew is read: {transitions:?}"
    );
    assert_eq!(
        pair.readout().awaited_directories,
        meta::AwaitedDirectories(vec![])
    );
    drop(subscription);
    pair.end(Member::Nexus);
}

#[test]
fn the_pair_ends_together_and_is_reaped() {
    Pair::started().end(Member::Translator);
}

#[test]
#[ignore = "translator-serving 0.1.1's pair launcher does not handle SIGTERM: ended so, it leaves the daemon and the translator running"]
fn a_launcher_ended_by_sigterm_ends_its_pair() {
    let mut pair = Pair::started();
    kill(
        Pid::from_raw(i32::try_from(pair.launcher.id()).expect("a process id")),
        ProcessSignal::SIGTERM,
    )
    .expect("the launcher is signalled");
    pair.launcher.wait().expect("the launcher ends");
    for (name, pid) in [("daemon", pair.nexus), ("translator", pair.translator)] {
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "the {name} ends with its launcher"
        );
    }
}
