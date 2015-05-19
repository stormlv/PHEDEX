package PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest;

use Moose;

has 'method'    => (is  => 'ro', isa => 'Str', required => 1);
has 'url'       => (is  => 'ro', isa => 'Str',  required => 1);
has 'arguments' => (is  => 'ro', isa => 'Ref');
has 'callback'  => (is  => 'ro', isa => 'Object', required => 1);

1;