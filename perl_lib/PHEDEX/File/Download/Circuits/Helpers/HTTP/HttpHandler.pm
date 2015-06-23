=head1 NAME

Helpers::HTTP::HttpHandler - Describes which event handles a URI request

=head1 DESCRIPTION

This object is used in the HttpServer to define which event needs to be 
triggered when a client makes a call to a given URI supported by the server.

An example handler would be:

Helpers::HTTP::HttpHandler(method => 'GET', uri => '/', eventName => 'postbackForGetHandler', session => $session);

This handler defines that the I<postbackForGetHandler> event should be called if a someone issues a GET request to the / URI

When constructing it, it requires:

    - the event name (and session) handling the URI
    
    - the method and URI to be handled

=cut

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