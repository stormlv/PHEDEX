package PHEDEX::Tests::File::Download::Circuits::Backend::NSI::TestStateMachine;

use strict;
use warnings;

use base 'PHEDEX::Core::Logging';

use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition;

use Test::More;

# Test the identifyNextTransition method
sub testIdentifyNextTransition {
    my ($stateMachine, $id, $result);
    
    my $msg = "TestStateMachine->testIdentifyNextTransition";
    $stateMachine = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine->new(verbose => 1);
    
    # Error: No transition defined in the state machine matches againts this message
    $id = "d005b619-16be-4312-82bf-4960ebdc6320";
    $result = $stateMachine->identifyNextTransition("This is not the expression you are looking for connectionId: $id");
    is($result, undef, "$msg: Cannot identify transition. Unmatched message");
    
    # Error: No transition defined in the state machine matches againts this message
    # Although the message is correct, the form of the connectionID passed is not
    $id = "d005b619-4312-82bf-4960eb6320";
    $result = $stateMachine->identifyNextTransition("Received an errorEvent for connectionId: $id");
    is($result, undef, "$msg: Cannot identify transition. Incorrect connection id");

    # Error: There is a valid transition defined in the state machine which matches against the message provided
    # The issue here is that the state machine is in a state which doesn't allow this transition 
    # (stateMachine->currentState != transitionFound->initialState)
    $id = "d005b619-16be-4312-82bf-4960ebdc6320";
    $result = $stateMachine->identifyNextTransition("Received reserveCommitConfirmed for connectionId: $id");
    is($result, undef, "$msg: Cannot identify transition. Transition identified but not allowed");
    
    # StateMachine->currentState = Created
    # Correctly identifies a possible transition to Error
    $id = "d005b619-16be-4312-82bf-4960ebdc6322";
    $result = $stateMachine->identifyNextTransition("Received an errorEvent for connectionId: $id");
    ok($result, "Was able to identify a transition based on this message");
    is($result->[0], $id, "$msg: Identified correct ID: $id");
    is($result->[1]->initialState, 'Created', "$msg: Correct initial state: ".$result->[1]->initialState);
    is($result->[1]->futureState, 'Error', "$msg: Correct future state: ".$result->[1]->futureState);
    is($result->[1]->isTerminal, 1, "$msg: Correct terminal state: ".$result->[1]->isTerminal);
    
    # StateMachine->currentState = Created
    # Correctly identifies a possible transition to AssignedId
    $id = "d005b619-16be-4312-82bf-4960ebdc6321";
    $result = $stateMachine->identifyNextTransition("Submitted reserve, new connectionId = $id");
    ok($result, "$msg: Was able to identify a transition based on this message");
    is($result->[0], $id, "$msg: Identified correct ID: $id");
    is($result->[1]->initialState, 'Created', "$msg: Correct initial state: ".$result->[1]->initialState);
    is($result->[1]->futureState, 'AssignedId', "$msg: Correct future state: ".$result->[1]->futureState);
    is($result->[1]->isTerminal, 0, "$msg: Correct terminal state: ".$result->[1]->isTerminal);
}

sub testDoTransition {
    my $msg = "TestStateMachine->testDoTransition";
    my $stateMachine = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine->new(verbose => 1);

    # Error: Cannot do transition since it's not defined in the state machine
    my $newTransition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'AssignedId', futureState => 'Created', requiredRegex => "Bla bla regex");
    my $newState = $stateMachine->doTransition($newTransition);
    ok(! $newState, "$msg: Did not do the transition. ".$newTransition->id()." is not defined in the state machine");
    
    # Error: Cannot do transition since the current state of the state machine doesn't match the transition initial state
    $newTransition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Confirmed', futureState => 'Error', requiredRegex => "Bla bla regex");
    $newState = $stateMachine->doTransition($newTransition);
    ok(! $newState, "$msg: Did not do the transition. ".$newTransition->id()." defined, but initial state doesn't match the current state of the state machine");
    
    # Transition to AssignedID
    my $result = $stateMachine->identifyNextTransition("Submitted reserve, new connectionId = d005b619-16be-4312-82bf-4960ebdc6321");
    $newState = $stateMachine->doTransition($result->[1]);
    is($newState, "AssignedId", "$msg: (Check1) Transitioned to AssignedID");
    is($stateMachine->currentState(), "AssignedId", "$msg: (Check2) Transitioned to AssignedID");
    ok(! $stateMachine->isInTerminalState(), "$msg: (Check3) Is not in terminal state");
    
    # Transition to Error state. Check if state machine is in terminal state
    $result = $stateMachine->identifyNextTransition("Received an errorEvent for connectionId: d005b619-16be-4312-82bf-4960ebdc6321");
    $newState = $stateMachine->doTransition($result->[1]);
    is($newState, "Error", "$msg: (Check1) Transitioned to Error");
    is($stateMachine->currentState(), "Error", "$msg: (Check2) Transitioned to Error");
    ok($stateMachine->isInTerminalState(), "$msg: (Check3) Is in terminal state");
}


sub testCorrectStateMachine {
    my $stateMachine = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine->new(verbose => 1);
    my $msg = "TestStateMachine->testDoTransition";
    
    my $transitionId = 'Created-to-AssignedId';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Created-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'AssignedId-to-Confirmed';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'AssignedId-to-ConfirmFail';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'AssignedId-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Confirmed-to-Commited';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Confirmed-to-CommitFail';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Confirmed-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Commited-to-Provisioned';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Commited-to-ProvisionFail';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Commited-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Provisioned-to-Active';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Provisioned-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Active-to-Terminated';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Active-to-Error';
    ok($stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId identified");
    $transitionId = 'Created-to-Active';
    ok(!$stateMachine->hasTransition($transitionId), "$msg: Transition $transitionId is not valid in the state machine ");
}

testIdentifyNextTransition();
testDoTransition();
testCorrectStateMachine();

done_testing();
1;