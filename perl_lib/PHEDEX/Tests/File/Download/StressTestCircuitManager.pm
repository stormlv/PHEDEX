package PHEDEX::Tests::File::Download::StressTestCircuitManager;

use strict;
use warnings;

use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::Tests::File::Download::Helpers::SessionCreation;
use POE;
use Test::More;

# The purpose of this test is to stress the CircuitManager in creating requests,
# establishing circuits, tearing them down and eventually removing old circuits from history
# The main loop runs once every 50ms
#   0ms         - request a circuit
#   ~10ms       - TEST: check that request is valid (exist in CM and on disk)
#   ~20ms       - a circuit is established (Backend induced delay is 20ms   - TIME_SIMULATION = 0.02)
#   ~30ms       - TEST: check that the circuit established is valid (exist in CM->{CIRCUITS} and on disk, and request was removed from disk) 
#   ~40ms       - the circuit is torn down (Circuit lifetime is 20ms        - LIFETIME = 0.02)
#   ~50ms       - TEST: check that the circuit was put offline (exists in CM->{CIRCUITS_OFFLINE} and on disk, and established circuits was removed from disk and from CM->{CIRCUITS})   

sub stressTestCircuitCreation {
    
    our $i = 0;
    
    # Used to     
    our $circuits = [   ['T2_ANSE_CERN_1', 'T2_ANSE_CERN_2'], 
                        ['T2_ANSE_CERN_1', 'T2_ANSE_CERN_Dev'],
                        ['T2_ANSE_CERN_2', 'T2_ANSE_CERN_1'],
                        ['T2_ANSE_CERN_2', 'T2_ANSE_CERN_Dev'],
                        ['T2_ANSE_CERN_Dev', 'T2_ANSE_CERN_1'],
                        ['T2_ANSE_CERN_Dev', 'T2_ANSE_CERN_2']];
                        
    # Main loop
    # It creates a circuit request, then sets up a timer to iCheckRequest
    sub iMainLoop {
        my ($circuitManager, $session) =  @_[ARG0, ARG1];
        
        my $fromNode = $circuits->[$i % 6][0];
        my $toNode =  $circuits->[$i % 6][1];
       
        POE::Kernel->post($session, 'requestCircuit', $fromNode, $toNode, 0.02);
        
        my $linkName = $fromNode."-to-".$toNode;
        
        POE::Kernel->delay(\&iCheckRequest => 0.01, $circuitManager, $linkName);                
        
        my $eventCount = POE::Kernel->get_event_count();
        
        print "event count $eventCount \n";
        
        $i++;        
        # Event reccurence every 60ms        
        POE::Kernel->delay(\&iMainLoop => 0.06, $circuitManager, $session);        
    }
    
    # Checks that a circuit has been requested, then sets up a timer to iCheckEstablished
    sub iCheckRequest {
        my ($circuitManager, $linkName) = @_[ARG0, ARG1];
        
        my $circuit = $circuitManager->{CIRCUITS}{$linkName};
        
        ok(defined $circuit, "stress test / iCheckRequest - Circuit exists in circuit manager");
        is($circuit->{STATUS}, CIRCUIT_STATUS_REQUESTING,"stress test / iCheckRequest - Circuit is in requesting state in circuit manager");
        
        my $path = $circuit->getSaveName();        
        ok($path  =~ m/requested/ && -e $path, "stress test / iCheckRequest - Circuit (in requesting state) exists on disk as well");
        
        POE::Kernel->delay(\&iCheckEstablished => 0.02, $circuitManager, $linkName);
    }
    
    # Checks that a circuit has been established, then sets up a timer to iCheckTeardown
    sub iCheckEstablished {
        my ($circuitManager, $linkName) = @_[ARG0, ARG1];
        
        my $circuit = $circuitManager->{CIRCUITS}{$linkName};
        
        ok(defined $circuit, "stress test / iCheckEstablished - Circuit exists in circuit manager");
        is($circuit->{STATUS}, CIRCUIT_STATUS_ONLINE,"stress test / iCheckEstablished - Circuit is in established state in circuit manager");
        
        my $path = $circuit->getSaveName();        
        ok($path  =~ m/online/ && -e $path, "stress test / iCheckRequest - Circuit (in established state) exists on disk as well");
        
        POE::Kernel->delay(\&iCheckTeardown => 0.02, $circuitManager, $circuit);
    }
    
    # Checks that a circuit has been put in history    
    sub iCheckTeardown {
        my ($circuitManager, $circuit) = @_[ARG0, ARG1];
    
        my $linkName = $circuit->getLinkName();                       
        ok(!defined $circuitManager->{CIRCUITS}{$linkName}, "stress test / iCheckTeardown - Circuit doesn't exist in circuit manager anymore");
        ok(defined $circuitManager->{CIRCUITS_HISTORY}{$linkName}{$circuit->{ID}}, "stress test / iCheckTeardown - Circuit exists in circuit manager history");
        
        is($circuit->{STATUS}, CIRCUIT_STATUS_OFFLINE,"stress test / iCheckTeardown - Circuit is in offline state in circuit manager");
        
        my $path = $circuit->getSaveName();        
        ok($path  =~ m/offline/ && -e $path, "stress test / iCheckRequest - Circuit (in offline state) exists on disk as well");
    }
    
    sub iCheckHistoryTrimming {
        my ($circuitManager) = $_[ARG0];
        
        my ($olderThanNeeded, @circuitsOffline);        
        
        &getdir($circuitManager->{CIRCUITDIR}."/offline", \@circuitsOffline);
        
        ok(scalar @{$circuitManager->{CIRCUITS_HISTORY_QUEUE}} <=  $circuitManager->{MAX_HISTORY_SIZE}, "There are no more than $circuitManager->{MAX_HISTORY_SIZE} circuits in HISTORY");
        ok(scalar @circuitsOffline <=  $circuitManager->{MAX_HISTORY_SIZE}, "There are no more than $circuitManager->{MAX_HISTORY_SIZE} circuits in HISTORY folder");
                
        POE::Kernel->delay(\&iCheckHistoryTrimming => 1, $circuitManager);
    }

    my ($circuitManager, $session) = setupCircuitManager(MINUTE, 'creating-circuit-requests.log', undef, 
                                                            [[\&iMainLoop, 0.01],
                                                             [\&iCheckRequest, undef],
                                                             [\&iCheckEstablished, undef],
                                                             [\&iCheckTeardown, undef],
                                                             [\&iCheckHistoryTrimming, 1]]);
    # We don't need all of the log messages                                                             
    $circuitManager->{VERBOSE} = 0;       
    
    $circuitManager->Logmsg('Testing events requestCircuit, handleRequestResponse and teardownCircuit');
    
    # Booking backend induced delay is 20ms
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.02;    
    $circuitManager->{SYNC_HISTORY_FOLDER} = 1;
        
    ### Run POE 
    POE::Kernel->run();
    
    print "The end\n";
}

stressTestCircuitCreation();

done_testing();

1;
