package PHEDEX::File::Download::Circuits::Helpers::Tasks::Task;

use Moose;

has 'task'          => (is  => 'ro', isa => 'Ref', required => 1);
has 'action'        => (is  => 'ro', isa => 'Ref');
has 'alarmId'       => (is  => 'rw', isa => 'Int');
has 'alarmTimeout'  => (is  => 'rw', isa => 'Int', default => 10);
1;
