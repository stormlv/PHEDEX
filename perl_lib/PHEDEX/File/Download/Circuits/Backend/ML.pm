package PHEDEX::File::Download::Circuits::Backend::ML;

use strict;
use warnings;

use base 'PHEDEX::File::Download::Circuits::Backend::Core::Core','PHEDEX::Core::Logging';

# PhEDEx imports
use PHEDEX::File::Download::Circuits::Backend::Core::IDC;
use PHEDEX::File::Download::Circuits::Backend::Helpers::HttpClient;
use PHEDEX::File::Download::Circuits::Backend::Helpers::HttpServer;
use PHEDEX::File::Download::Circuits::Constants;

use HTTP::Status qw(:constants);
use POE;
use Switch;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my %params = (
        HTTP_CLIENT         =>  undef,

        ML_ADDRESS          => "http://pccit16.cern.ch:8080/phedex",        # Default address for the ML server
        ML_REQUEST          => "/",                                         # URI used to request a circuit
        ML_STATUS_POLL      => "/",                                         # URI used to check the status of a given circuit
        ML_TEARDOWN         => "/",                                         # URI used to request a teardown

        ML_LOOP_DELAY       => 5,                                           # Polling interval to see if we got the circuit or not

        EXCHANGE_MESSAGES   =>  "JSON",                                     # Type of messages we want to exchange

        DELAYS              => undef,                                       # Hash keeping track of all the polls that we have to do 
    );

    my %args = (@_);

    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);

    # Start the HTTP client
    $self->{HTTP_CLIENT} = PHEDEX::File::Download::Circuits::Backend::Helpers::HttpClient->new();
    $self->{HTTP_CLIENT}->spawn();

    bless $self, $class;
    return $self;
}

# Init POE events
# declare event 'processToolOutput' which is passed as a postback to External
# call super
sub _poe_init
{
    my ($self, $kernel, $session) = @_;

    $kernel->state('handleRequestReply', $self);
    $kernel->state('handleTeardownReply', $self);
    $kernel->state('requestStatusPoll', $self);
    $kernel->state('handlePollReply', $self);

    # Needed since we're calling this subroutine directly instead of passing through POE
    my @superArgs; @superArgs[KERNEL, SESSION] = ($kernel, $session); shift @superArgs;

    # Parent does the main initialization of POE events
    $self->SUPER::_poe_init(@superArgs);
}

sub backendRequestCircuit {
    my ($self, $kernel, $session, $circuit, $requestCallback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    
    # Setup the object sent to ML
    my $requestObject = {
        REQUEST_TYPE    => 'CREATE',                        # Needed in case the URI for creation and teardown are the same
        ID              =>  $circuit->{ID},                 # ID of the circuit which we want to establish
        FROM            =>  $circuit->{PHEDEX_TO_NODE}, 
        TO              =>  $circuit->{PHEDEX_FROM_NODE},
        OPTIONS         => {
                            BANDWIDTH   => $circuit->{BANDWIDTH_REQUESTED},
                            LIFETIME    => $circuit->{LIFETIME}
                           },
    };

    # Create a (POE) post back to this session
    # $self and $circuit are provided as arguments, as well as $requestCallback.
    # Since we're working in the same session as the CircuitManager, $requestCallback only has the event name and is not a real postback
    my $postback = $session->postback("handleRequestReply", $self, $circuit, $requestCallback);
    
    # Make the actual POST request
    $self->{HTTP_CLIENT}->httpRequest(
                "POST",                                         # Method used 
                $self->{ML_ADDRESS}.$self->{ML_REQUEST},        # Address and method URI
                [$self->{EXCHANGE_MESSAGES}, $requestObject],   # Type of messages which are exchanged; object that is sent 
                $postback);                                     # We need to provide a postback in order to get the reply
}

# Handler for the initial request (post) reply
sub handleRequestReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit, $requestCallback) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # First check if the (http) post succeeded or not
    if ($resultCode != HTTP_OK) {
        $self->Logmsg("There has been an error in requesting the circuit");
        # Propagate the error back to the circuit manager
        $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
        return;
    }

    # If we get to here it means that ML accepted our request
    # We need to start a loop to poll and see if the circuit request was accepted
    my $delayID = $kernel->delay_set("requestStatusPoll", $self->{ML_LOOP_DELAY}, $circuit, $requestCallback);
    $self->{DELAYS}{$circuit->{ID}} = $delayID;
}

# This event will get triggered periodically (given by ML_LOOP_DELAY)
# and it will check to see if a circuit request has succeeded or not 
sub requestStatusPoll {
    my ($self, $kernel, $session, $circuit, $requestCallback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];

    # Create the polling input (simple stuff)
    my $pollRequest = {
        ID      => $circuit->{ID}
    };

    # Create a (POE) post back to this session
    # Based on this reply, we'll know if the request is still in progress or has failed
    my $postback = $pollRequest, $session->postback("handlePollReply", $self, $circuit, $requestCallback);
    
    # Check the status
    $self->{HTTP_CLIENT}->httpRequest(
            "GET",                                          # Method used (GET in this case)
            $self->{ML_ADDRESS}.$self->{ML_STATUS_POLL},    # Address and method URI
            $postback);                                     # We need to provide a postback in order to get the reply
}

# Handler for the poll (get) reply
sub handlePollReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit, $requestCallback) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # First check if the (http) get succeeded or not
    if ($resultCode != HTTP_OK || ! defined $resultObject || ! defined $resultObject->{STATUS}) {
        $self->Logmsg("There has been an error in requesting the circuit");
        # Propagate the error back to the circuit manager
        $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
        return;
    }

    # Get the delay ID (alarm id) which was previously started
    my $delayID = $self->{DELAYS}{$circuit->{ID}};

    # Case based on the status that we got from the (ML) server
    my $status = $resultObject->{STATUS};
    switch($status) {
        case "REQUESTING" {
            # Schedule another poll
            $kernel->delay_adjust($delayID, $self->{ML_LOOP_DELAY}, $circuit, $requestCallback);
        }
        case "ESTABLISHED" {
            # Process the return values in a format that the CircuitManager will like
            my $returnValues = {
                FROM_IP     =>  $resultObject->{FROM_IP},
                TO_IP       =>  $resultObject->{TO_IP},
                BANDWIDTH   =>  $resultObject->{BANDWIDTH},
            };

            # Inform the CircuitManager that the request was successful
            $kernel->post($session, $requestCallback, $circuit, $returnValues, CIRCUIT_REQUEST_SUCCEEDED);
            
            # Remove timer from this backend and from the POE
            delete $self->{DELAYS}{$circuit->{ID}};
            $kernel->alarm_remove($delayID);
        }
        case "FAILED" {
            # The request has failed... There isn't much to do, except inform the CircuitManager
            $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
            
            # Remove timer from this backend and from the POE
            delete $self->{DELAYS}{$circuit->{ID}};
            $kernel->alarm_remove($delayID);
        }
        # TODO: More status messages:
        #   DENIED_TOO_GREEDY: Informs the CM that the request has been denied (and gives a reason why: we requested a BW that's too large or life that's too long)
        #           CM can re-adjust and remake the request
        #   DENIED_MAX_CAPACITY: Informs the CM that the circuit backend cannot allocate another circuit. It also provides the time for the next available slot. CM can blacklist the path until then
        # etc...
    }
}

# Request the teardown of a particular circuit...
sub backendTeardownCircuit {
    my ( $self, $kernel, $session, $circuit ) = @_[ OBJECT, KERNEL, SESSION, ARG0 ];

    # Setup the object sent to ML
    my $requestObject = {
        REQUEST_TYPE    => 'TEARDOWN',
        ID              =>  $circuit->{ID},
    };

    my $postback = $session->postback("handleTeardownReply", $self, $circuit);

    # Make the actuall POST request
    $self->{HTTP_CLIENT}->httpRequest(
                "POST",                                             # Method used 
                $self->{ML_ADDRESS}.$self->{ML_TEARDOWN},           # Address and method URI
                [$self->{EXCHANGE_MESSAGES}, $requestObject],       # Type of messages which are exchanged; object that is sent
                $postback);                                         # We need to provide a postback in order to get the reply
}

# Handler for the teardown reply
sub handleTeardownReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # First check if the (http) post succeeded or not
    if ($resultCode != HTTP_OK) {
        $self->Logmsg("There has been an error in tearing down the circuit");
        # Umm, c$$p? 
        # TODO: Reschedule teardown attempt
        return;
    }
}

1;