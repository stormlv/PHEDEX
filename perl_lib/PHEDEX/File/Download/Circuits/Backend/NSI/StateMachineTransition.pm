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