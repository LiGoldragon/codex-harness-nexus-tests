use flow::FlowId;
use harness::{
    BackgroundWork, Description, EndReason, Event, Failure, Flow, Observed, State, Tokens,
    Transitionable, WorkId, WorkKind,
};

#[test]
fn event_transitions_follow_the_ruled_state_machine() {
    let mut observed = Observed {
        flow_id: FlowId("a64934".into()),
        state: State::Dry,
        tokens: Tokens(0),
        background_work_vector: vec![],
    };
    let transitions = [
        (Event::Started, State::Dry),
        (Event::PromptSubmitted, State::Flowing(Flow::Working)),
        (Event::PermissionAsked, State::Flowing(Flow::Blocked)),
        (Event::ToolUsed, State::Flowing(Flow::Working)),
        (Event::TurnEnded, State::Flowing(Flow::Rested)),
        (Event::PromptSubmitted, State::Flowing(Flow::Working)),
        (Event::Interrupted, State::Flowing(Flow::Rested)),
        (Event::PromptSubmitted, State::Flowing(Flow::Working)),
        (
            Event::Failed(Failure("synthetic failure".into())),
            State::Flowing(Flow::Rested),
        ),
        (Event::Ended(EndReason::Exited), State::Ended),
    ];
    for (event, state) in transitions {
        observed.after(event);
        assert_eq!(observed.state, state);
    }
    observed.after(Event::ContextSize(Tokens(183950)));
    assert_eq!(observed.tokens, Tokens(183950));
    assert_eq!(observed.state, State::Ended);
}

#[test]
fn a_turn_waits_for_its_last_background_work() {
    let first_work = BackgroundWork {
        work_id: WorkId("first".into()),
        work_kind: WorkKind::Shell,
        description: Description("synthetic first work".into()),
    };
    let second_work = BackgroundWork {
        work_id: WorkId("second".into()),
        work_kind: WorkKind::Subagent,
        description: Description("synthetic second work".into()),
    };
    let mut observed = Observed {
        flow_id: FlowId("a64934".into()),
        state: State::Flowing(Flow::Working),
        tokens: Tokens(0),
        background_work_vector: vec![],
    };
    observed.after(Event::InFlight(vec![first_work.clone()]));
    observed.after(Event::WorkStarted(second_work.clone()));
    observed.after(Event::TurnEnded);
    assert_eq!(observed.state, State::Flowing(Flow::Waiting));
    observed.after(Event::WorkEnded(WorkId("unknown".into())));
    assert_eq!(observed.background_work_vector.len(), 2);
    observed.after(Event::WorkEnded(first_work.work_id));
    assert_eq!(observed.state, State::Flowing(Flow::Waiting));
    observed.after(Event::WorkEnded(second_work.work_id));
    assert_eq!(observed.state, State::Flowing(Flow::Rested));
    assert!(observed.background_work_vector.is_empty());
}
