//! The Nexus with its translator, in one process: a configured Nexus, an
//! identity producer that answers one flow id, and a rollout written row by
//! row from the recorded fixtures. The rows are recorded; their sequence is
//! synthetic, as fixtures/README.md says.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use codex_harness_nexus::{
    Channelled, Followed, HarnessNexus, Locking, Opening, Operation, Performing, Remembering,
    Rollout, SessionRecord, SessionsWatch, Store, WatchWord, Watching,
};
use codex_harness_translator::{CodexHarnessTranslator, TranslatorSettings};
use flow::FlowId;
use futures_util::StreamExt;
use harness::{Event, FilePath, Flow, Observed, Response, Session, State};
use meta_signal_codex_harness_nexus::{
    Capacity, ConfigurationRefusal, FlowsRoot, IdentityProducer, SessionsDirectories, Set,
    ThreadDatabases,
};
use signal::Frame;
use signal_codex_harness_nexus::{
    ByteOffset, Changed, Hook, IdentityFailure, Output, ParseFailure, ReportFailure,
    SessionStart_Data, Transition_Data,
};
use translator_serving::ValidTextBudget;

const SESSION_META: &str =
    "{\"type\":\"session_meta\",\"payload\":{\"id\":\"01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be\"}}\n";
const TASK_STARTED: &str = include_str!("../fixtures/task-started.jsonl");
const TOKEN_COUNT: &str = include_str!("../fixtures/token-count.jsonl");
const TOKEN_COUNT_NULL: &str = include_str!("../fixtures/token-count-null.jsonl");
const TASK_COMPLETE: &str = include_str!("../fixtures/task-complete-response-synthetic.jsonl");
const TASK_COMPLETE_NULL: &str = include_str!("../fixtures/task-complete-null.jsonl");
const TURN_ABORTED: &str = include_str!("../fixtures/turn-aborted.jsonl");
const SESSION: &str = "01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be";
const FLOW: &str = "4c85ce";
/// The longest the kernel's next word is waited for.
const DEADLINE: Duration = Duration::from_secs(60);

/// A Nexus in this process, and the words of its watch, which the test
/// follows itself, as the daemon's watch thread does.
struct Fixture {
    directory: tempfile::TempDir,
    nexus: HarnessNexus<CodexHarnessTranslator>,
    words: Receiver<WatchWord>,
}

trait Fixtured: Sized {
    fn fixture(producer: &str) -> Self;
    fn reopened(self) -> Self;
    fn path(&self, name: &str) -> PathBuf;
    fn configure(&self);
    fn started(&self, path: Option<&Path>) -> Result<(), Output>;
    fn append(&self, path: &Path, text: &str);
    fn read(&self, path: &Path) -> Vec<Changed>;
    /// Performs one operation, which the lane fails nowhere, and answers
    /// the changes it committed.
    fn performed(&self, operation: Operation) -> Vec<Changed>;
    fn record(&self) -> SessionRecord;
    fn text(&self, name: &str) -> String;
    /// Follows the kernel's words until `done` holds after one, and answers
    /// what that one told.
    fn follow_until(&self, done: &dyn Fn(&Self, &Followed) -> bool) -> Followed;
    /// Where the watch of the one configured sessions directory rests.
    fn resting(&self) -> Option<PathBuf>;
    fn awaited(&self) -> Vec<FilePath>;
}

/// A Nexus made fresh on a Memory, and the words of its watch.
trait Made: Sized {
    fn made(memory: &Path) -> (Self, Receiver<WatchWord>);
}

impl Made for HarnessNexus<CodexHarnessTranslator> {
    fn made(memory: &Path) -> (Self, Receiver<WatchWord>) {
        let (sessions_watch, words) = SessionsWatch::channelled();
        (
            HarnessNexus {
                store: Store::open(memory).unwrap(),
                translator: CodexHarnessTranslator::in_process(),
                serial: Mutex::new(()),
                observations: Mutex::new(None),
                configurations: Mutex::new(None),
                sessions_watch: Mutex::new(sessions_watch),
            },
            words,
        )
    }
}

/// The translator's emissions, answered in this process.
trait InProcess {
    fn in_process() -> Self;
}

impl InProcess for CodexHarnessTranslator {
    fn in_process() -> Self {
        Self {
            settings: RwLock::new(TranslatorSettings {
                text_budget: ValidTextBudget::MAXIMUM,
            }),
        }
    }
}

fn transition(event: Event, state: State) -> Changed {
    Changed::Transition(Transition_Data {
        flow_id: FlowId(FLOW.into()),
        event,
        state,
    })
}

impl Fixtured for Fixture {
    fn fixture(producer: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("flows")).unwrap();
        fs::create_dir(directory.path().join("sessions")).unwrap();
        let script = directory.path().join("flow-id");
        fs::write(&script, producer).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let (nexus, words) =
            HarnessNexus::made(&directory.path().join("state").join("codex-harness.memory"));
        Self {
            directory,
            nexus,
            words,
        }
    }

    fn reopened(self) -> Self {
        let Self {
            directory, nexus, ..
        } = self;
        drop(nexus);
        let (nexus, words) =
            HarnessNexus::made(&directory.path().join("state").join("codex-harness.memory"));
        Self {
            directory,
            nexus,
            words,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn configure(&self) {
        use meta_signal_codex_harness_nexus::Answering;
        let text = |path: PathBuf| path.to_string_lossy().into_owned();
        for set in [
            Set::FlowsRoot(FlowsRoot(text(self.path("flows")))),
            Set::IdentityProducer(IdentityProducer(text(self.path("flow-id")))),
            Set::SessionsDirectories(SessionsDirectories(vec![FilePath(text(
                self.path("sessions"),
            ))])),
            Set::ThreadDatabases(ThreadDatabases(vec![])),
            Set::SubscriberCapacity(Capacity(16)),
        ] {
            assert_eq!(self.nexus.configure(&set), Ok(set.clone()));
        }
    }

    fn started(&self, path: Option<&Path>) -> Result<(), Output> {
        use signal_codex_harness_nexus::Answering;
        self.nexus.report(&Hook::SessionStart(SessionStart_Data {
            session: Session(SESSION.into()),
            file_path_option: path.map(|path| FilePath(path.to_string_lossy().into_owned())),
        }))
    }

    fn append(&self, path: &Path, text: &str) {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
    }

    fn read(&self, path: &Path) -> Vec<Changed> {
        let Rollout::Watched(watch) = self.record().rollout else {
            panic!("the rollout at {} is not watched", path.display());
        };
        self.performed(Operation::Reading(watch))
    }

    fn performed(&self, operation: Operation) -> Vec<Changed> {
        let performed = self.nexus.perform(operation).unwrap();
        assert_eq!(performed.lane_failure_option, None);
        performed.changed_vector
    }

    fn record(&self) -> SessionRecord {
        self.nexus
            .store
            .recall(Session(SESSION.into()))
            .unwrap()
            .expect("the session is remembered")
    }

    fn text(&self, name: &str) -> String {
        self.path(name).to_string_lossy().into_owned()
    }

    fn follow_until(&self, done: &dyn Fn(&Self, &Followed) -> bool) -> Followed {
        loop {
            let word = self
                .words
                .recv_timeout(DEADLINE)
                .expect("the kernel tells the watch");
            let followed = self.nexus.follow(word);
            assert_eq!(followed.failures, vec![], "the watch moves without failing");
            if done(self, &followed) {
                return followed;
            }
        }
    }

    fn resting(&self) -> Option<PathBuf> {
        self.nexus
            .sessions_watch
            .locked()
            .current
            .as_ref()?
            .placements[0]
            .resting
            .clone()
    }

    fn awaited(&self) -> Vec<FilePath> {
        use codex_harness_nexus::ConfigurationReading;
        self.nexus.readout().unwrap().awaited_directories.0
    }
}

const PRODUCER: &str = "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$2\" = --flows-root ] && [ \"$CODEX_SESSION_ID\" = 01a1e4cb-c2d2-7d91-86ea-0b4c85ce34be ] || exit 2\necho 4c85ce\n";

#[test]
fn an_unconfigured_nexus_refuses_a_report_typed() {
    let fixture = Fixture::fixture(PRODUCER);
    assert_eq!(
        fixture.started(None),
        Err(Output::ReportRefused(ReportFailure::Unconfigured))
    );
}

#[test]
fn a_configuration_value_is_checked_before_it_is_kept() {
    use meta_signal_codex_harness_nexus::{Answering, Output as MetaOutput};
    let fixture = Fixture::fixture(PRODUCER);
    assert_eq!(
        fixture
            .nexus
            .configure(&Set::SubscriberCapacity(Capacity(0))),
        Err(MetaOutput::ConfigureRefused(
            ConfigurationRefusal::InvalidCapacity(Capacity(0))
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .configure(&Set::FlowsRoot(FlowsRoot("relative/flows".into()))),
        Err(MetaOutput::ConfigureRefused(
            ConfigurationRefusal::InvalidPath(FilePath("relative/flows".into()))
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .configuration()
            .unwrap()
            .flows_root_option,
        None
    );
}

#[test]
fn the_nexus_refuses_a_text_budget_as_its_translators_setting() {
    use meta_signal_codex_harness_nexus::{
        Answering, DepthLimit, NodeLimit, Output as MetaOutput, TextBudget,
    };
    let fixture = Fixture::fixture(PRODUCER);
    assert_eq!(
        fixture.nexus.configure(&Set::TextBudget(TextBudget {
            node_limit: NodeLimit(1024),
            depth_limit: DepthLimit(32),
        })),
        Err(MetaOutput::ConfigureRefused(
            ConfigurationRefusal::TranslatorSetting
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .configuration()
            .unwrap()
            .text_budget_option,
        None
    );
}

#[test]
fn a_hook_before_its_session_started_is_unregistered() {
    use signal_codex_harness_nexus::Answering;
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    assert_eq!(
        fixture
            .nexus
            .report(&Hook::PostToolUse(Session(SESSION.into()))),
        Err(Output::ReportRefused(ReportFailure::Unregistered(Session(
            SESSION.into()
        ))))
    );
}

#[test]
fn a_refusing_identity_producer_leaves_the_session_unidentified() {
    let fixture = Fixture::fixture("#!/bin/sh\necho no >&2\nexit 2\n");
    fixture.configure();
    let refused = fixture.started(None);
    let Err(Output::ReportRefused(ReportFailure::Unidentified(IdentityFailure::Refused(refused)))) =
        refused
    else {
        panic!("the claim is refused: {refused:?}");
    };
    assert_eq!(refused.exit_status.0, 2);
    assert!(
        fixture
            .nexus
            .store
            .recall(Session(SESSION.into()))
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_named_rollout_attaches_and_its_rows_tell_the_turn() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    assert_eq!(fixture.started(Some(&rollout)), Ok(()));
    let rollout_path = FilePath(rollout.to_string_lossy().into_owned());
    assert_eq!(
        fixture.record().rollout,
        Rollout::Pending(rollout_path.clone())
    );
    assert_eq!(fixture.record().observed.state, State::Dry);

    fixture.append(&rollout, SESSION_META);
    fixture.append(&rollout, TASK_STARTED);
    fixture.append(&rollout, TOKEN_COUNT);
    let changes = fixture.performed(Operation::Discovering(rollout_path));
    let Changed::Transition(context) = &changes[1] else {
        panic!("the second change is a transition: {changes:?}");
    };
    let Event::ContextSize(tokens) = context.event else {
        panic!("the token count tells the context size: {changes:?}");
    };
    assert_eq!(
        changes,
        vec![
            transition(Event::PromptSubmitted, State::Flowing(Flow::Working)),
            transition(Event::ContextSize(tokens), State::Flowing(Flow::Working)),
        ]
    );

    fixture.append(&rollout, TASK_COMPLETE);
    let Event::Responded(response) = (match &fixture.read(&rollout)[..] {
        [Changed::Transition(responded), Changed::Transition(ended)] => {
            assert_eq!(ended.event, Event::TurnEnded);
            assert_eq!(ended.state, State::Flowing(Flow::Rested));
            responded.event.clone()
        }
        changes => panic!("a completed task responds, then ends its turn: {changes:?}"),
    }) else {
        panic!("the first change is the response");
    };
    assert_eq!(response, Response(response.0.clone()));

    let record = fixture.record();
    assert_eq!(
        record.observed,
        Observed {
            flow_id: FlowId(FLOW.into()),
            state: State::Flowing(Flow::Rested),
            tokens,
            background_work_vector: vec![],
        }
    );
    let Rollout::Watched(watch) = record.rollout else {
        panic!("the rollout is watched");
    };
    let length = fs::metadata(&rollout).unwrap().len() as i64;
    assert_eq!(watch.byte_offset, ByteOffset(length));
}

#[test]
fn a_row_not_yet_ended_waits_and_a_second_turn_end_is_dropped() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    fixture.append(&rollout, TASK_STARTED);
    let path = FilePath(rollout.to_string_lossy().into_owned());
    fixture.performed(Operation::Discovering(path));

    let (head, tail) = TASK_COMPLETE_NULL.split_at(TASK_COMPLETE_NULL.len() / 2);
    fixture.append(&rollout, head);
    assert_eq!(fixture.read(&rollout), vec![]);
    fixture.append(&rollout, tail);
    assert_eq!(
        fixture.read(&rollout),
        vec![transition(Event::TurnEnded, State::Flowing(Flow::Rested))]
    );
    fixture.append(&rollout, TASK_COMPLETE_NULL);
    assert_eq!(fixture.read(&rollout), vec![]);
    fixture.append(&rollout, TOKEN_COUNT_NULL);
    fixture.append(&rollout, TASK_STARTED);
    fixture.append(&rollout, TURN_ABORTED);
    assert_eq!(
        fixture.read(&rollout),
        vec![
            transition(Event::PromptSubmitted, State::Flowing(Flow::Working)),
            transition(Event::Interrupted, State::Flowing(Flow::Rested)),
        ]
    );
}

#[test]
fn a_malformed_row_is_recorded_and_read_past() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    fixture.append(&rollout, "");
    fixture.performed(Operation::Discovering(FilePath(
        rollout.to_string_lossy().into_owned(),
    )));
    fixture.append(&rollout, "{not json\n");
    fixture.append(&rollout, TASK_STARTED);
    let changes = fixture.read(&rollout);
    let [Changed::Malformed(malformed), started] = &changes[..] else {
        panic!("a malformed row, then the next row: {changes:?}");
    };
    assert_eq!(malformed.byte_offset, ByteOffset(0));
    assert!(matches!(
        malformed.parse_failure,
        ParseFailure::InvalidJson(_)
    ));
    assert_eq!(
        started,
        &transition(Event::PromptSubmitted, State::Flowing(Flow::Working))
    );
    assert_eq!(fixture.read(&rollout), vec![]);
}

#[test]
fn an_unnamed_rollout_is_found_by_its_first_row() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    fixture.started(None).unwrap();
    assert_eq!(fixture.record().rollout, Rollout::Unlocated);
    let stranger = fixture.path("sessions").join("other.jsonl");
    fixture.append(
        &stranger,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"another-session\"}}\n",
    );
    assert_eq!(
        fixture.performed(Operation::Discovering(FilePath(
            stranger.to_string_lossy().into_owned()
        ))),
        vec![]
    );
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    fixture.append(&rollout, SESSION_META);
    fixture.append(&rollout, TASK_STARTED);
    assert_eq!(
        fixture.performed(Operation::Discovering(FilePath(
            rollout.to_string_lossy().into_owned()
        ))),
        vec![transition(
            Event::PromptSubmitted,
            State::Flowing(Flow::Working)
        )]
    );
}

#[test]
fn the_memory_outlives_the_process() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    fixture.append(&rollout, TASK_STARTED);
    fixture.performed(Operation::Discovering(FilePath(
        rollout.to_string_lossy().into_owned(),
    )));
    let before = fixture.record();
    let fixture = fixture.reopened();
    assert_eq!(fixture.record(), before);
    assert_eq!(
        fixture.nexus.store.configuration().unwrap().capacity_option,
        Some(Capacity(16))
    );
}

#[test]
fn a_subscriber_opens_on_the_state_and_follows_each_change() {
    use signal_codex_harness_nexus::Answering;
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let rollout = fixture.path("sessions").join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    let Ok(mut stream) = fixture.nexus.subscribe() else {
        panic!("a configured Nexus is subscribed to");
    };
    fixture.append(&rollout, TASK_STARTED);
    fixture.performed(Operation::Discovering(FilePath(
        rollout.to_string_lossy().into_owned(),
    )));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let opened = runtime.block_on(stream.next());
    let Some(Frame::Opened(observations)) = opened else {
        panic!("the stream opens on the state");
    };
    assert_eq!(observations.0.len(), 1);
    assert_eq!(observations.0[0].state, State::Dry);
    assert_eq!(
        runtime.block_on(stream.next()),
        Some(Frame::Changed(transition(
            Event::PromptSubmitted,
            State::Flowing(Flow::Working)
        )))
    );
}

#[test]
fn the_kernel_names_a_rollout_written_in_a_nested_sessions_directory() {
    use codex_harness_nexus::Waking;
    let fixture = Fixture::fixture(PRODUCER);
    let day = fixture.path("sessions").join("2026").join("11");
    fs::create_dir_all(&day).unwrap();
    fixture.configure();
    let rollout = day.join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    fixture.append(&rollout, TASK_STARTED);
    let path = FilePath(rollout.to_string_lossy().into_owned());
    let written = fixture
        .follow_until(&|_, followed| followed.written.contains(&path))
        .written;
    let operations = fixture.nexus.woken(written).unwrap();
    assert_eq!(operations, vec![Operation::Discovering(path.clone())]);
    for operation in operations {
        fixture.performed(operation);
    }
    assert!(matches!(fixture.record().rollout, Rollout::Watched(_)));
    assert_eq!(
        fixture.nexus.woken(vec![path]).unwrap(),
        match fixture.record().rollout {
            Rollout::Watched(watch) => vec![Operation::Reading(watch)],
            rollout => panic!("watched: {rollout:?}"),
        }
    );
    assert_eq!(
        fixture.nexus.startup().unwrap().len(),
        1,
        "one look at startup reads the watched rollout"
    );
}

#[test]
fn an_absent_sessions_directory_is_awaited_and_its_watch_moves_down_until_it_is_there() {
    use codex_harness_nexus::Waking;
    use meta_signal_codex_harness_nexus::Answering;
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let later = fixture.path("later");
    let codex = later.join(".codex");
    let sessions = codex.join("sessions");
    let directory = FilePath(sessions.to_string_lossy().into_owned());
    let set = Set::SessionsDirectories(SessionsDirectories(vec![directory.clone()]));
    assert_eq!(
        fixture.nexus.configure(&set),
        Ok(set.clone()),
        "a directory not yet there is configured"
    );
    assert_eq!(fixture.awaited(), vec![directory.clone()]);
    assert_eq!(
        fixture.resting(),
        Some(fixture.directory.path().to_path_buf()),
        "the watch rests on the nearest ancestor there"
    );
    let rollout = sessions.join("rollout.jsonl");
    fixture.started(Some(&rollout)).unwrap();
    for level in [&later, &codex] {
        fs::create_dir(level).unwrap();
        fixture.follow_until(&|fixture, _| fixture.resting().as_ref() == Some(level));
        assert_eq!(fixture.awaited(), vec![directory.clone()]);
    }
    // The directory appears holding its rollout already.
    let staged = fixture.path("staging-sessions");
    fs::create_dir(&staged).unwrap();
    fixture.append(&staged.join("rollout.jsonl"), SESSION_META);
    fs::rename(&staged, &sessions).unwrap();
    let followed = fixture.follow_until(&|_, followed| !followed.arrived.is_empty());
    assert_eq!(followed.arrived, vec![directory.clone()]);
    assert_eq!(fixture.resting(), Some(sessions.clone()));
    assert_eq!(
        fixture.awaited(),
        vec![],
        "nothing is awaited once it is there"
    );
    assert_eq!(
        fixture.nexus.startup().unwrap(),
        vec![Operation::Discovering(FilePath(
            rollout.to_string_lossy().into_owned()
        ))],
        "the look on its arrival finds the rollout already inside"
    );
}

#[test]
fn a_sessions_directory_removed_is_awaited_again_and_watched_once_made_anew() {
    let fixture = Fixture::fixture(PRODUCER);
    fixture.configure();
    let sessions = fixture.path("sessions");
    let directory = FilePath(fixture.text("sessions"));
    assert_eq!(fixture.awaited(), vec![]);
    fs::remove_dir(&sessions).unwrap();
    fixture.follow_until(&|fixture, _| !fixture.awaited().is_empty());
    assert_eq!(fixture.awaited(), vec![directory.clone()]);
    assert_eq!(
        fixture.resting(),
        Some(fixture.directory.path().to_path_buf())
    );
    fs::create_dir(&sessions).unwrap();
    let followed = fixture.follow_until(&|_, followed| !followed.arrived.is_empty());
    assert_eq!(followed.arrived, vec![directory.clone()]);
    assert_eq!(fixture.awaited(), vec![]);
    // Removed and made again before the watch hears of it: the directory
    // made anew is watched, and told as come to be there.
    fs::remove_dir(&sessions).unwrap();
    fs::create_dir(&sessions).unwrap();
    let followed = fixture.follow_until(&|_, followed| !followed.arrived.is_empty());
    assert_eq!(followed.arrived, vec![directory]);
    assert_eq!(fixture.resting(), Some(sessions));
    assert_eq!(fixture.awaited(), vec![]);
}
