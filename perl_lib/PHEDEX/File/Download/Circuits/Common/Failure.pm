package PHEDEX::File::Download::Circuits::Common::Failure;

use Moose;

has 'comment'       => (is  => 'ro', isa => 'Str',  required => 1);
has 'errorCode'     => (is  => 'rw', isa => 'Int');
has 'time'          => (is  => 'ro', isa => 'Num',  required => 1);
has 'faultObject'   => (is  => 'ro', isa => 'HashRef',  required => 0);

1;