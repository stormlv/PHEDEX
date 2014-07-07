package PHEDEX::Tests::File::Download::StressTestCircuitManager;

use strict;
use warnings;

use PHEDEX::Tests::File::Download::Helpers::SessionCreation;
use POE;
use Test::More;

sub stressTestCircuitCreation {
    
    sub iStressTestCreation {
        my ($circuitManager, $session) = @_;
        
        POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', 0.1);
        POE::Kernel->delay(\&iStressTestCreation => 0.1, $circuitManager, $session);
        
    }
    
    my ($circuitManager, $session) = setupCircuitManager(30, 'creating-circuit-requests.log', undef, undef, 
                                                            [[\&iStressTestCreation, 0.1]]);       
    $circuitManager->Logmsg('Testing events requestCircuit, handleRequestResponse and teardownCircuit');
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.01; 
        
    ### Run POE 
    POE::Kernel->run(); 
}

stressTestCircuitCreation();

done_testing();

1;
