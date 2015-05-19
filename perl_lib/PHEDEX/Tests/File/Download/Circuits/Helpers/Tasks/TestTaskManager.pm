package PHEDEX::Tests::File::Download::Circuits::Helpers::Tasks::TestTaskManager;

use strict;
use warnings;

use PHEDEX::File::Download::Circuits::Helpers::Tasks::Task;
use PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager;

use POE;
use Test::More;

POE::Session->create(
    inline_states => {
        _start          => \&_start,
         handleAction   => \&handleAction,
         finalTest      => \&finalTest,
    }
);

our $allOutput;

sub _start {
    my ($kernel, $session) = @_[KERNEL, SESSION];

    # Create the task manager which will launch all the tasks
    my $tasker = PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager->new();
    # Create the action which is going to be called on STDOUT by TaskManager
    my $postback = $session->postback('handleAction');
    
    $kernel->delay_add( finalTest => 1.0);
    # Start a command 
    $tasker->startCommand('ls', $postback, 1);
}

sub handleAction {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];

    my $pid = $arguments->[EXTERNAL_PID];
    my $eventName = $arguments->[EXTERNAL_EVENTNAME];
    my $output = $arguments->[EXTERNAL_OUTPUT];
    
    if (defined $output) {
        $allOutput = $output;
        print "$output\n";
    }
}

sub finalTest {
    is($allOutput, 'TestTaskManager.pm', "TestTaskManager: Correctly used ls :)")
}

POE::Kernel->run();

done_testing();

1;