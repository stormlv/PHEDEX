package PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair;

use Moose;

has 'arg'     => (is  => 'rw', isa => 'Str', required => 1);
has 'value'   => (is  => 'rw', isa => 'Str', required => 1);

1;