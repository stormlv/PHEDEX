=head1 NAME

Backend::NSI::StateMachineTransition - Holds the attributes for a transition in the state machine

=head1 DESCRIPTION

When constructing it, it requires:

    initial and future states: both are StateMachineStates objects
    
    regex: this is the required regular expression, which needs to be matched
    in order to move from initial to future state
    
Although it is optional, one should also include the "isTerminal" parameter at construction

=cut

package PHEDEX::File::Download::Circuits::Backend::NSI::StateMachineTransition;

use Moose;

use PHEDEX::File::Download::Circuits::Common::EnumDefinitions;

# Define enums
use Moose::Util::TypeConstraints;
    enum 'StateMachineStates',  NSI_STATES;
no Moose::Util::TypeConstraints;

has 'id'            => (is  => 'rw', isa => 'Str');
has 'initialState'  => (is  => 'ro', isa => 'StateMachineStates', required => 1);
has 'isTerminal'    => (is  => 'rw', isa => 'Bool', default => 0);
has 'futureState'   => (is  => 'ro', isa => 'StateMachineStates', required => 1);
has 'requiredRegex' => (is  => 'ro', isa => 'Str', required => 1);

sub BUILD {
    my $self = shift;
    $self->id($self->initialState."-to-".$self->futureState);
}

1;