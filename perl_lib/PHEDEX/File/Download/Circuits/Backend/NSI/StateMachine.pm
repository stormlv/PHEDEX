package PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;

use Moose;

use base 'PHEDEX::Core::Logging', 'Exporter';

use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition;
use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;

use constant CONNECTION_ID_REGEX => "(([\\d\\w]{8})((-[\\d\\w]{4}){3})-([\\d\\w]{12}))";
our @EXPORT = qw(TRANSITIONS CONNECTION_ID_REGEX);

use constant TRANSITIONS => ["Submitted reserve, new connectionId = ".CONNECTION_ID_REGEX, 
                             "Received reserveConfirmed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received reserveFailed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received reserveCommitConfirmed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received reserveCommitFailed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received provisionConfirmed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received provisionFailed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received dataPlaneStateChange for connectionId: ".CONNECTION_ID_REGEX,
                             "Received terminationConfirmed for connectionId: ".CONNECTION_ID_REGEX,
                             "Received an errorEvent for connectionId: ".CONNECTION_ID_REGEX];

use constant {
    REGEX_ASSIGNED_ID           =>  TRANSITIONS->[0],
    REGEX_CONFIRMED             =>  TRANSITIONS->[1],
    REGEX_CONFIRM_FAIL          =>  TRANSITIONS->[2],
    REGEX_COMMITTED             =>  TRANSITIONS->[3],
    REGEX_COMMIT_FAIL           =>  TRANSITIONS->[4],
    REGEX_PROVISIONED           =>  TRANSITIONS->[5],
    REGEX_PROVISION_FAIL        =>  TRANSITIONS->[6],
    REGEX_ACTIVE                =>  TRANSITIONS->[7],
    REGEX_TERMINATED            =>  TRANSITIONS->[8],
    REGEX_GENERIC_FAIL          =>  TRANSITIONS->[9],
};

# Define enums
use Moose::Util::TypeConstraints;
    enum 'StateMachineStates',  NSI_STATES;
no Moose::Util::TypeConstraints;

has 'currentState'      => (is  => 'rw', isa => 'StateMachineStates', default => 'Created', 
                            trigger => sub { 
                                            my ($self, $object, $objectOld) = @_;
                                            my $transition = $self->getTransition($objectOld."-to-".$object);
                                            $self->isInTerminalState($transition->isTerminal);
                            });
has 'transitions'       => (is  => 'ro', isa => 'HashRef[PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition]',
                            traits  => ['Hash'], 
                            handles => {addTransition       => 'set',
                                        getTransition       => 'get',
                                        hasTransition       => 'exists',
                                        getAllTransitionIDs => 'keys',
                                        getAllTransitions   => 'values'});
has 'isInTerminalState' => (is  => 'rw', isa => 'Bool', default => '0');
has 'verbose'           => (is  => 'rw', isa => 'Bool', default => '0');

# Static method
sub isValidMessage {
    my $message = shift;
    foreach my $transition (@{TRANSITIONS()}) {
        my @matches = $message =~ /$transition/;
        if (@matches) {
                my $connectionID = $matches[0];
                return $connectionID;
         }
    }
    return undef;
}

sub BUILD {
    my $self = shift;
    
    my $transition;
    
    # Created->AssignedId
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Created', futureState => "AssignedId", requiredRegex => REGEX_ASSIGNED_ID);
    $self->addTransition($transition->id, $transition);
    
    # AssignedId->Confirmed
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'AssignedId', futureState => "Confirmed", requiredRegex => REGEX_CONFIRMED);
    $self->addTransition($transition->id, $transition);
    
    # AssignedId->ConfirmFail
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'AssignedId', futureState => "ConfirmFail", isTerminal => 1, requiredRegex => REGEX_CONFIRM_FAIL);
    $self->addTransition($transition->id, $transition);
    
    # Confirmed->Commited
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Confirmed', futureState => "Commited", requiredRegex => REGEX_COMMITTED);
    $self->addTransition($transition->id, $transition);
    
    # Confirmed->CommitFail
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Confirmed', futureState => "CommitFail", isTerminal => 1, requiredRegex => REGEX_COMMIT_FAIL);
    $self->addTransition($transition->id, $transition);
    
    # Commited->Provisioned
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Commited', futureState => "Provisioned", requiredRegex => REGEX_PROVISIONED);
    $self->addTransition($transition->id, $transition);
    
    # Commited->ProvisionFail
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Commited', futureState => "ProvisionFail", isTerminal => 1, requiredRegex => REGEX_PROVISION_FAIL);
    $self->addTransition($transition->id, $transition);
    
    # Provisioned->Active
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Provisioned', futureState => "Active", requiredRegex => REGEX_ACTIVE);
    $self->addTransition($transition->id, $transition);
    
    # Active->Terminated
    $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState => 'Active', futureState => "Terminated", isTerminal => 1, requiredRegex => REGEX_TERMINATED);
    $self->addTransition($transition->id, $transition);
    
    # In addition to this all states can go into a generic error
    foreach my $storedTransition ($self->getAllTransitions()) {
        $transition = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition->new(initialState  => $storedTransition->initialState, 
                                                                                                  futureState   => "Error", 
                                                                                                  isTerminal    => 1,
                                                                                                  requiredRegex => REGEX_GENERIC_FAIL);
        $self->addTransition($transition->id, $transition);
    }
}

# Try to identify a transition from the current state based on the message provided
# It looks to all transitions defined in the state machine and attempts to find one
# for which the message matches the transition required regex. If this is found
# it verifies that this is a valid transition from the current state of the state machine
# If it is, it return the connection ID (extracted from the message) and transition 
# which was previously identified
sub identifyNextTransition {
    my ($self, $message) = @_;
    my $msg = "StateMachine->identifyNextTransition";
    foreach my $transition ($self->getAllTransitions()) {
        my $regex = $transition->requiredRegex();
        my @matches = $message =~ /$regex/;
        if (@matches) {
            $self->Logmsg("$msg: Found a transition (".$transition->id.") which matches the message provided") if $self->verbose;
            if ($transition->initialState() eq $self->currentState()) {
                $self->Logmsg("$msg: This transition is valid based on the current state") if $self->verbose;
                my $connectionID = $matches[0];
                return [$connectionID, $transition];
            }
        }
    }
    return undef;
}

# Try to set the next state based on a given transition
sub doTransition {
    my ($self, $transition) = @_;
    my $msg = "StateMachine->doTransition";
    
    if (! defined $transition) {
        $self->Logmsg("$msg: Invalid parameter was supplied");
        return undef;
    }
    if (! $self->hasTransition($transition->id())) {
        $self->Logmsg("$msg: This transition (".$transition->id().") is not possible for this state machine");
        return undef;
    }
    if ($self->currentState ne $transition->initialState) {
        $self->Logmsg("$msg: Cannot transition to next state. Check that the current state (".$self->currentState().") matches the transition initial state (".$transition->initialState().")");
        return undef;
    }

    $self->currentState($transition->futureState);
    return $self->currentState;
}

1;