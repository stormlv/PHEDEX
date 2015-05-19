package PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler;

use Moose;

use Moose::Util::TypeConstraints;
    subtype 'Uri', as 'Str', where {substr($_, 0, 1) eq "/"}, message { "URI should begin with a /"};
no Moose::Util::TypeConstraints;

has 'id'        => (is  => 'rw', isa => 'Str');
has 'eventName' => (is  => 'ro', isa => 'Str',      required => 1);
has 'uri'       => (is  => 'ro', isa => 'Uri',      required => 1);
has 'method'    => (is  => 'ro', isa => 'Str',      required => 1);
has 'session'   => (is  => 'ro', isa => 'Object',   required => 1);
has 'arguments' => (is  => 'ro', isa => 'ArrayRef', default => sub { [ ] }, traits  => ['Array'], handles => {getAllArguments => 'elements'});

sub BUILD {
    my $self = shift;
    $self->id($self->method.$self->uri);
}

sub getCallback {
    my $self = shift;
    return $self->session->postback($self->eventName, $self->getAllArguments);
}

1;