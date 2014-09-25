package PHEDEX::Tests::File::Download::TestHttpControl;

use strict;
use warnings;

use IO::File;
use File::Copy qw(move);
use POE;
use POSIX;
use Test::More;

use PHEDEX::File::Download::Circuits::Backend::Helpers::HttpClient;
use PHEDEX::File::Download::Circuits::Circuit;
use PHEDEX::File::Download::Circuits::CircuitManager;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::Tests::File::Download::Helpers::ObjectCreation;
use PHEDEX::Tests::File::Download::Helpers::SessionCreation;

# This test checks to see if the CircuitManager can be controlled via the HTTP Server that it exposes
# This will request the creation and the teardown of a circuit through the web interface
sub testHttpCircuitLifecycle {

    our $userAgent;

    sub iStartUserAgent {
        $userAgent = PHEDEX::File::Download::Circuits::Backend::Helpers::HttpClient->new();
        $userAgent->spawn();
    }
    
    sub iRequestHttp {
        my ($kernel, $session, $circuitManager) = @_[KERNEL, SESSION, ARG0];

        my $request = {
            FROM_NODE   => "T2_ANSE_CERN_1",
            TO_NODE     => "T2_ANSE_CERN_2",
        };

        $kernel->state(iTestPostback => \&iTestPostback);
        my $postback = $session->postback("iTestPostback");
        $userAgent->httpRequest("POST", "http://localhost:8080/createCircuit", ["JSON", $request], $postback);
    };

    sub iTestPostback {
        my @result = @_;
        ok(@result, "http client - received reply");
    }

    # Intermediate test that checks that requests were actually created
    sub iTestHttpRequest {
        my $circuitManager = $_[ARG0];

        my $linkName = "T2_ANSE_CERN_1-to-T2_ANSE_CERN_2";
        my $circuit = $circuitManager->{CIRCUITS}{$linkName};

        is($circuit->{STATUS}, CIRCUIT_STATUS_REQUESTING, "circuit manager / requestCircuit - circuit status in circuit manager is correct");

        my $partialID = substr($circuit->{ID}, 1, 8);
        my $time = $circuit->{REQUEST_TIME};
        my $fileReq = $baseLocation."/data/requested/$linkName-$partialID-".formattedTime($time);

        ok(-e $fileReq, "circuit manager / requestCircuit - circuit has been requested");

        my ($openedCircuit, $returnCode) = &openCircuit($fileReq);
        is($returnCode, CIRCUIT_OK, "circuit manager / requestCircuit - was able to open saved state for circuit");
        ok($openedCircuit, "circuit manager / requestCircuit - was able to open saved state for circuit");

        is($openedCircuit->{PHEDEX_FROM_NODE}, 'T2_ANSE_CERN_1', "circuit manager / requestCircuit - circuit from node ok");
        is($openedCircuit->{PHEDEX_TO_NODE}, "T2_ANSE_CERN_2", "circuit manager / requestCircuit - circuit to node ok");
    }

    sub iGetInfoHttp {
        my ($kernel, $session, $circuitManager) = @_[KERNEL, SESSION, ARG0];
        
        $kernel->state(iTestGetInfoCircuits => \&iTestGetInfoCircuits);
        
        $userAgent->httpRequest("GET", "http://localhost:8080/getInfo", {REQUEST => "CIRCUITS"}, $session->postback("iTestGetInfoCircuits"));
    }

    sub iTestGetInfoCircuits {
        my ($initialArgs, $postArgs) = @_[ARG0, ARG1];
        
        my $object = $postArgs->[0];
        my $code = $postArgs->[1];
        my $response = $postArgs->[2];
        
        ok($postArgs, "http client - received reply");
        ok($object, "http client - received reply, found sent object");
        is($code, 200, "http client - received reply, http status code matches");
        ok($response, "http client - received reply, found http reponse");
        
        my $circuit = $object->{"T2_ANSE_CERN_1-to-T2_ANSE_CERN_2"};
        
        is($circuit->{PHEDEX_FROM_NODE}, "T2_ANSE_CERN_1", "http client - object test1");
        is($circuit->{PHEDEX_TO_NODE}, "T2_ANSE_CERN_2", "http client - object test2");
        is($circuit->{LIFETIME}, undef, "http client - object test3");
    }

    sub iTeardownHTTP {
        my ($kernel, $session, $circuitManager) = @_[KERNEL, SESSION, ARG0];

        my $request = {
            FROM_NODE   => "T2_ANSE_CERN_1",
            TO_NODE     => "T2_ANSE_CERN_2",
        };
        
        my $postback = $session->postback("iTestPostback");
        $userAgent->httpRequest("POST", "http://localhost:8080/removeCircuit", ["JSON", $request], $postback);
    };
    
    sub iTestTeardown {
        my $circuitManager = $_[ARG0];

        my $linkName = "T2_ANSE_CERN_1-to-T2_ANSE_CERN_2";
        my $circuitID = $circuitManager->{CIRCUITS_HISTORY}{$linkName};
        my $circuit = $circuitID->{(keys %{$circuitID})[0]};

        is($circuit->{STATUS}, CIRCUIT_STATUS_OFFLINE, "circuit manager / teardownCircuit - circuit status in circuit manager is correct");

        my $partialID = substr($circuit->{ID}, 1, 8);
        my $establishedtime = $circuit->{ESTABLISHED_TIME};
        my $offlinetime = $circuit->{LAST_STATUS_CHANGE};

        my $fileEstablished = $baseLocation."/data/online/$linkName-$partialID-".formattedTime($establishedtime);
        my $fileOffline = $baseLocation."/data/offline/$linkName-$partialID-".formattedTime($offlinetime);

        ok(!-e $fileEstablished, "circuit manager / teardownCircuit - request for circuit has been removed");
        ok(-e $fileOffline, "circuit manager / teardownCircuit - circuit has been established");

        my ($openedCircuit, $returnCode) = &openCircuit($fileOffline);
        is($returnCode, CIRCUIT_OK, "circuit manager / established - was able to open saved state for circuit");
        ok($openedCircuit, "circuit manager / established - was able to open saved state for circuit");
    }

    sub iStop {
        my $circuitManager = $_[ARG0];
        $circuitManager->stop();
        $userAgent->unspawn();
    }

    my ($circuitManager, $session) = setupCircuitManager(3, 'http-control.log', undef,
                                                        [[\&iStartUserAgent, 0.1],              # Start HTTP Client
                                                         [\&iRequestHttp, 0.2],                 # Request a circuit via the web interface
                                                         [\&iTestHttpRequest, 0.4],             # Test request
                                                         [\&iGetInfoHttp, 0.7],                 # Get info from the circuit manager via the web interface
                                                         [\&iTeardownHTTP, 1],                  # Request the teardown via the web interface 
                                                         [\&iTestTeardown, 1.5],                # Test teardown
                                                         [\&iStop, 2]],                         # Shutdown nicely
                                                        1); # Enable HTTP Control

    $circuitManager->Logmsg('Testing events requestCircuit, handleRequestResponse and teardownCircuit');
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.1; # Wait some time before actually establishing the circuit

    ### Run POE
    POE::Kernel->run();

}

File::Path::rmtree("$baseLocation".'/logs', 1, 1) if (-d "$baseLocation".'/logs');
File::Path::make_path("$baseLocation".'/logs', { error => \my $err});

testHttpCircuitLifecycle();

done_testing();

1;