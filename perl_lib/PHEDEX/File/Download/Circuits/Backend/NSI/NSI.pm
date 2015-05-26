package PHEDEX::File::Download::Circuits::Backend::NSI::NSI;

use Moose;
extends 'PHEDEX::File::Download::Circuits::Backend::Core::Core';

use base 'PHEDEX::Core::Logging';

# PhEDEx imports
use PHEDEX::File::Download::Circuits::Backend::NSI::Action;
use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;
use PHEDEX::File::Download::Circuits::ResourceManager::ResourceManagerConstants;
use PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager;


# Other imports
use Data::UUID;
use LWP::Simple;
use POE;
use Switch;

use constant {
    REQUEST     => "Request",
    TEARDOWN    => "Teardown",
};

has 'taskManager'       => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager', default => sub { PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager->new() });
has 'actionHandler'     => (is  => 'rw', isa => '');
has 'timeout'           => (is  => 'rw', isa => 'Int', default => 120);
has 'actionQueue'       => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Backend::NSI::Action]', 
                            traits => ['Array'],
                            handles => {queueAction     => 'push', 
                                        dequeueAction   => 'shift', 
                                        actionQueueSize => 'count'});
has 'currentAction'     => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Action');
has 'reservations'      => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::Backend::NSI::Reservation]', 
                            traits => '[Hash]',  
                            handles => {addReservation      => 'set',
                                        getReservation      => 'get',
                                        removeReservation   => 'delete',
                                        hasReservation      => 'exists'});
has 'nsiToolLocation'   => (is  => 'rw', isa => 'Str', default => '/data/NSI/CLI');
has 'nsiTool'           => (is  => 'rw', isa => 'Str', default => 'nsi-cli-1.2.1-one-jar.jar');
has 'nsiToolJavaFlags'  => (is  => 'rw', isa => 'Str', default =>   '-Xmx256m -Djava.net.preferIPv4Stack=true '.
                                                                    '-Dlog4j.configuration=file:./config/log4j.properties ',
                                                                    '-Dcom.sun.xml.bind.v2.runtime.JAXBContextImpl.fastBoot=true ',
                                                                    '-Dorg.apache.cxf.JDKBugHacks.defaultUsesCaches=true ');
has 'defaultProvider'   => (is  => 'rw', isa => 'Str', default => 'provider.script');   # Provider should also have the truststore containing the aggregator server certificats (store password is in: provider-client-https-cc.xml)
has 'defaultRequester'  => (is  => 'rw', isa => 'Str', default => 'requester.script');  # Requester should also provide the truststore with his certificate and key (store and key password are in: requester-server-http.xml)
has 'session'           => (is  => 'rw', isa => 'Ref');
has 'uuid'              => (is  => 'rw', isa => 'Data::UUID', default => sub {new Data::UUID});
has 'verbose'           => (is  => 'rw', isa => 'Int');

# Init POE events
# - declare event 'processToolOutput' which is passed as a postback to External
# - call super
sub _poe_init
{
    my ($self, $kernel, $session) = @_;

    # Create the action which is going to be called on STDOUT by External
    $kernel->state('processToolOutput', $self);
    $self->session($session);
    $self->actionHandler($session->postback('processToolOutput'));

    # Parent does the main initialization of POE events
    $self->SUPER::_poe_init($kernel, $session);
    
    # Launch an instance of the NSI CLI
    chdir $self->nsiToolLocation;
    $self->nsiToolPid = $self->taskManager->startCommand("java ".$self->nsiToolJavaFlags." -jar ".$self->nsiTool, $self->actionHandler, $self->timeout);
    $self->taskManager->getTaskByPID($self->nsiToolPid)->put('nsi override');
}

sub getRequestScript {
    my ($self, $providerName, $requesterName, $scriptName) = @_;
    
    $providerName = $self->defaultProvider if ! defined $providerName;
    $requesterName = $self->defaultRequester if ! defined $requesterName;
    
    if (! defined $scriptName) {
        $self->Alert("The script to load has not been provided")
    }
    
    my $script = "";
    $script .= "script --file ".$self->nsiToolLocation."/scripts/provider/$providerName\n";
    $script .= "script --file ".$self->nsiToolLocation."/scripts/requester/$requesterName\n";

    return $script;
}

sub backendRequestResource {
    my ($self, $circuit, $requestCallback) = @_[ OBJECT, ARG0, ARG1];
    $self->queueAction(REQUEST, $circuit, $requestCallback);
}

sub backendTeardownResource {
    my ($self, $circuit) = @_[ OBJECT, ARG0];
    $self->queueAction(TEARDOWN, $circuit);
}

# Creates a new action based on the action type provided (Request, Teardown)
# This action is queued and will be executed after the rest of the pending actions have been completed
# This restriction is due to us using the NSI CLI tool instead of having a native implementation
sub queueThenExecuteAction {
    my ($self, $actionType, $circuit, $requestCallback) = @_;

    if (! defined $actionType || ! defined $circuit || ! defined $circuit) {
        $self->Logmsg("NSI->queueThenExecuteAction: Invalid parameters have been supplied");
        return;
    }

    my $action = PHEDEX::File::Download::Circuits::Backend::NSI::Action->new(id => $self->{UUID}->create(), 
                                                                             type => $actionType, 
                                                                             circuit => $circuit, 
                                                                             callback => $requestCallback);

    $self->queueAction($action);
    $self->executeNextAction();
}

# Executes the next action in the queue if there's no other pending action
sub executeNextAction {
    my $self = shift;

    if (defined $self->currentAction) {
        $self->Logmsg("Other actions are still pending... Will execute new action when appropiate");
        return;
    }

    if ($self->actionQueueSize == 0) {
        $self->Logmsg("The action queue is empty...");
        return;
    }
    
    # Pick the next action from the queue
    $self->currentAction($self->dequeueAction());
    my $action = $self->currentAction;
    
    switch ($action->type) {
        case REQUEST {
            my $reservation = PHEDEX::File::Download::Circuits::Backend::NSI::Reservation->new();
            $reservation->callback($action->callback);
            $reservation->updateParameters($action->circuit);

            $self->currentAction->reservation($reservation);

            # Set the reservation parameters into the CLI
            my $reserveCommands = $reservation->getReservationSetterScript();

            # And request the circuit
            push (@{$reserveCommands}, "nsi reserve\n");
            
            $self->sendToCLI($reserveCommands);
        }

        # TODO: Implement the MODIFY function
        # For now just teardown and request new one...
        
        case TEARDOWN {
            # Get the reservation which was assigned to this circuit and send the CLI commands to terminate reservation
            my $reservation = $self->currentAction->reservation;
            my $terminationCommands = $reservation->getTerminationScript();
            
            # TODO: Should probably remove after getting the OK from the NSI controller
            $self->removeReservation($reservation->connectionId);
            $self->sendToCLI($terminationCommands);
        }
    }

    $self->currentAction(undef);
    $self->executeNextAction();
}

# Send commands to the NSI CLI
sub sendToCLI {
    my ($self, $script) = @_;
    
    if (! defined $script || $script eq "") {
        $self->Logmsg("Cannot execute an empty script");
        return;
    }
    
    # Get the task info
    my $nsiTool = $self->taskManager->getTaskByPID($self->nsiToolPid);
    
    foreach my $line (@{$script}) {
        $nsiTool->put($line);
    }
}

sub processToolOutput {
    my ($self, $kernel, $session, $arguments) = @_[OBJECT, KERNEL, SESSION, ARG1];

    my $pid = $arguments->[EXTERNAL_PID];
    my $task = $arguments->[EXTERNAL_TASK];
    my $eventName = $arguments->[EXTERNAL_EVENTNAME];
    my $output = $arguments->[EXTERNAL_OUTPUT];

    switch ($eventName) {
        case 'handleTaskStdOut' {
            $self->Logmsg("NSI CLI($pid): $output") if $self->verbose;
            
            # Identify the connection ID that the output talks about
            my $regex = CONNECTION_ID_REGEX;
            my @matches = $output =~ /$regex/;
            if (! @matches) {
                $self->Logmsg("Couldn't find any connection ID");
                return;
            }
            my $connectionId = $matches[0];
            
            my $reservation;
            
            if ($self->hasReservation($connectionId)) {
                $reservation = $self->getReservation($connectionId);
            } else {
                if ($self->currentAction->reservation->connectionId eq $connectionId) {
                    $reservation = $self->currentAction->reservation;
                } else {
                    $self->Logmsg("Couldn't find any reservation matching this ID");
                    return;
                }
            }
            
            my $result = $reservation->stateMachine->identifyNextTransition($output);
            if (!defined $result) {
                $self->Logmsg("We received a message intended for reservation with connectionID: $connectionId which is invalid.");
                $reservation->removeReservation($connectionId);
                return;
            };
            
            my $transition = $result->[1];
            $reservation->stateMachine->doTransition();

            switch($reservation->stateMachine->currentState) {
                case 'AssignedId' {
                    $reservation->connectionId($connectionId);
                    $self->addReservation($connectionId, $reservation);
                }
                # If the reservation was held, then commit it
                case 'Confirmed' {
                    my $script = $reservation->getOverrideScript();
                    push (@{$script}, "nsi commit\n");
                    $self->sendToCLI($script);
                }
                
                # If the reservation was committed, then provision it
                case 'Commited' {
                    my $script = $reservation->getOverrideScript();
                    push (@{$script}, "nsi provision\n");
                    $self->sendToCLI($script);
                }
                
                # Reservation is now active (dataplane should now work)
                case 'Active' {
                    $self->Logmsg("Circuit creation succeeded");
                    POE::Kernel->post($self->session, $reservation->callback, $reservation->circuit, undef, CIRCUIT_REQUEST_SUCCEEDED);
                }

                # Reservation has been terminated
                case 'Terminated' {
                     $self->Logmsg("Circuit terminated");
                     $reservation->removeReservation($connectionId);
                     # TODO: Maybe warn the Reservation Manager as well?
                }

                # The reservation failed for whatever reason
                case ['Error', 'ConfirmFail', 'CommitFail', 'ProvisionFail'] {
                    $self->Logmsg("Circuit creation failed");
                    $reservation->removeReservation($connectionId);
                    # The circuit failed, we need to make sure we clean up everything
                    my $terminationCommands = $reservation->getTerminationScript();
                    $self->sendToCLI($terminationCommands);
                    POE::Kernel->post($self->session, $reservation->callback, $reservation->circuit, undef, CIRCUIT_REQUEST_FAILED);
                }
            }
        }

        case 'handleTaskStdError' {
            $self->Logmsg("An error has occured with NSI CLI ($pid): $output");
        }

        case 'handleTaskSignal' {
            $self->Logmsg("NSI CLI tool is being terminated ");
        }
    }
}

1;