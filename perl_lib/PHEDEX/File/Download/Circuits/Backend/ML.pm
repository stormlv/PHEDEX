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
        
        ML_ADDRESS          => "http://pccit16.cern.ch:8080/phedex",
        ML_REQUEST          => "/",
        ML_STATUS_POLL      => "/",
        ML_TEARDOWN         => "/", 

        ML_LOOP_DELAY       => 5,
        
        EXCHANGE_MESSAGES   =>  "JSON",
        
        DELAYS              => undef,
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
        ID      =>  $circuit->{ID},
        FROM    =>  $circuit->{PHEDEX_TO_NODE},
        TO      =>  $circuit->{PHEDEX_FROM_NODE},
        OPTIONS => {
            BANDWIDTH   => $circuit->{BANDWIDTH_REQUESTED},
            LIFETIME    => $circuit->{LIFETIME}
        },
    };

    # Make the actuall POST request
    $self->{HTTP_CLIENT}->httpRequest(
                "POST",                                                                         # Method used 
                $self->{ML_ADDRESS}.$self->{ML_REQUEST},                                        # Address and method URI
                [$self->{EXCHANGE_MESSAGES}, $requestObject],                                   # Type of messages which are exchanged; object that is sent 
                $session->postback("handleRequestReply", $self, $circuit, $requestCallback));   # Create a postback to this session
}

# Handler for the initial request (post) reply
sub handleRequestReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit, $requestCallback) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # First check if the request succeeded or not
    if ($resultCode != HTTP_OK) {
        $self->Logmsg("There has been an error in getting the circuit");
        $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
        return;
    }

    # If we get to here it means that ML accepted our request
    
    # We need to start a loop to poll and see if the circuit was accepted
    my $delayID = $kernel->delay_set("requestStatusPoll", $self->{ML_LOOP_DELAY}, $circuit, $requestCallback);
    $self->{DELAYS}{$circuit->{ID}} = $delayID;
}


sub requestStatusPoll {
    my ($self, $kernel, $session, $circuit, $requestCallback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];

    # Create the polling input (simple stuff)
    my $pollRequest = {
        ID      => $circuit->{ID}
    };

    # Check the status
    $self->{HTTP_CLIENT}->httpRequest(
            "GET", 
            $self->{ML_ADDRESS}.$self->{ML_STATUS_POLL},
            $pollRequest, $session->postback("handlePollReply", $self, $circuit, $requestCallback));
}

# Handler for the poll (get) reply
sub handlePollReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit, $requestCallback) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};
    
        # First check if the poll succeeded or not
    if ($resultCode != HTTP_OK || ! defined $resultObject || ! defined $resultObject->{STATUS}) {
        $self->Logmsg("There has been an error in getting the circuit");
        $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
        return;
    }

    my $delayID = $self->{DELAYS}{$circuit->{ID}};

    my $status = $resultObject->{STATUS};
    switch($status) {
        case "REQUESTING" {
            # Schedule another poll
            $kernel->delay_adjust($delayID, $self->{ML_LOOP_DELAY}, $circuit, $requestCallback);
        }
        case "ESTABLISHED" {
            my $returnValues = {
                FROM_IP     =>  $resultObject->{TO_IP},
                TO_IP       =>  $resultObject->{TO_IP},
                BANDWIDTH   =>  $resultObject->{BANDWIDTH},
            };
            
            $kernel->post($session, $requestCallback, $circuit, $returnValues, CIRCUIT_REQUEST_SUCCEEDED);
            
            # Remove from CircuitManager and remove from POE
            delete $self->{DELAYS}{$circuit->{ID}};
            $kernel->alarm_remove($delayID);
        }
        case "FAILED" {
            $kernel->post($session, $requestCallback, $circuit, undef, CIRCUIT_REQUEST_FAILED);
            
            # Remove from CircuitManager and remove from POE
            delete $self->{DELAYS}{$circuit->{ID}};
            $kernel->alarm_remove($delayID);
        }
    }
}

sub backendTeardownCircuit {
    my ( $self, $kernel, $session, $circuit ) = @_[ OBJECT, KERNEL, SESSION, ARG0 ];

    # Setup the object sent to ML
    my $requestObject = {
        ID      =>  $circuit->{ID},
    };

    # Make the actuall POST request
    $self->{HTTP_CLIENT}->httpRequest(
                "POST",                                                                         # Method used 
                $self->{ML_ADDRESS}.$self->{ML_TEARDOWN},                                       # Address and method URI
                [$self->{EXCHANGE_MESSAGES}, $requestObject],                                   # Type of messages which are exchanged; object that is sent 
                $session->postback("handleTeardownReply", $self, $circuit));   # Create a postback to this session
}

sub handleTeardownReply {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($self, $circuit) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # First check if the request succeeded or not
    if ($resultCode != HTTP_OK) {
        $self->Logmsg("There has been an error in tearing down the circuit");
        # Umm, c$$p? 
        # TODO: Reschedule teardown attempt
        return;
    }
}

1;