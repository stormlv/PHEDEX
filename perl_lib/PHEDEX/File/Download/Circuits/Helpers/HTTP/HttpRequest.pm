=head1 NAME

Helpers::HTTP::HttpRequest - Class used to describe an HTTP request in HttpClient

=head1 DESCRIPTION

When constructing it, it requires:

    -method type: POST or GET
    
    -url to which the request is being sent
    
    -a callback to the external method handling the response

Additionally, arguments can be specified as well.

In the case of GET, the arguments need to be in a hash form

In the case of POST, the arguments attribute is given as an array:

- the first element represents the type of data being pushed: FROM, TEXT, JSON

- second element is the hash with the data being passed on 

=cut

package PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest;

use Moose;

use Moose::Util::TypeConstraints;
    enum 'HttpRequestType', [qw(GET POST)];
no Moose::Util::TypeConstraints;

has 'method'    => (is  => 'ro', isa => 'HttpRequestType', required => 1);
has 'url'       => (is  => 'ro', isa => 'Str',  required => 1);
has 'arguments' => (is  => 'ro', isa => 'Ref');
has 'callback'  => (is  => 'ro', isa => 'Object', required => 1);

1;