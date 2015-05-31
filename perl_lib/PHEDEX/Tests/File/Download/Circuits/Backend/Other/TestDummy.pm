package PHEDEX::Tests::File::Download::Circuits::Backend::Other::Dummy;

use strict;
use warnings;

use POE;
use Test::More;

use PHEDEX::File::Download::Circuits::Backend::Other::Dummy;
use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

#  $nodes[0] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_Dev', netName => '137.138.42.16');
#    $nodes[1] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_0', netName => '188.184.134.192');
#    $nodes[2] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_1', netName => '128.142.135.112');
#    $nodes[3] = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_2', netName => '127.0.0.1');

sub setupSession {
    my $dummyBackend = shift;
    
    my $states;

    $states->{_start} = sub {
        my ($kernel, $session) = @_[KERNEL, SESSION];
        $dummyBackend->Logmsg("Starting a POE test session (id=",$session->ID,")");
        $dummyBackend->_poe_init($kernel, $session);
        
        # Create callbacks
        my $testCreationSuccess = $session->postback("testCreationSuccess"); 
        my $testCreationFailure = $session->postback("testCreationFailure");
        my $testUpdateSuccess = $session->postback("testUpdateSuccess");
        my $testUpdateFailure = $session->postback("testUpdateFailure");
        my $testTerminationSuccess = $session->postback("testTerminationSuccess");
        
        my $request = PHEDEX::File::Download::Circuits::Backend::Core::ResourceRequest->new( siteA => "T2_ANSE_CERN_Dev", siteB => "T2_ANSE_CERN_0", callback => $testCreationSuccess);
        my $failedRequest = PHEDEX::File::Download::Circuits::Backend::Core::ResourceRequest->new( siteA => "T2_ANSE_CERN_Dev", siteB => "T2_ANSE_CERN_0", callback => $testCreationFailure);
        
        my $node1 = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_Dev', netName => '137.138.42.16');
        my $node2 = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'T2_ANSE_CERN_0', netName => '188.184.134.192');
        my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $node1, nodeB => $node2, type => 'Layer2');
        my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy',
                                                                                               resourceType    => 'Circuit',
                                                                                               path => $path);
    
        # Delayed request of a circuit
        $dummyBackend->timeSimulation(1);
        $kernel->call($session, 'backendRequestResource', $request);
        $dummyBackend->timeSimulation(-1);
        $kernel->call($session, 'backendRequestResource', $failedRequest);
        $dummyBackend->timeSimulation(1);
        $kernel->call($session, 'backendUpdateResource', $resource, $testUpdateSuccess);
        $dummyBackend->timeSimulation(-1);
        $kernel->call($session, 'backendUpdateResource', $resource, $testUpdateFailure);
    };

    $states->{testCreationSuccess} = \&testCreationSuccess;
    $states->{testCreationFailure} = \&testCreationFailure;
    $states->{testUpdateSuccess} = \&testUpdateSuccess;
    $states->{testUpdateFailure} = \&testUpdateFailure;
    $states->{testTerminationSuccess} = \&testTerminationSuccess;
    
    my $session = POE::Session->create(inline_states => $states);

    return $session;
}

sub testCreationSuccess {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    is($status, REQUEST_SUCCEEDED, "TestDummy->testCreationSuccess: Creation request succeeded");
    $kernel->delay('backendTeardownResource' => 1, $resource, $session->postback("testTerminationSuccess"));
}

sub testCreationFailure {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    
    is($status, REQUEST_FAILED, "TestDummy->testCreationFailure: Creation request failed");
}

sub testUpdateSuccess {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    
    is($status, UPDATE_SUCCEEDED, "TestDummy->testUpdateSuccess: Update succeeded");
}

sub testUpdateFailure {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    
    is($status, UPDATE_FAILED, "TestDummy->testUpdateFailure: Update failed");
}

sub testTerminationSuccess {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    
    is($status, TERMINATE_SUCCEEDED, "TestDummy->testTerminationSuccess: Termination succeeded");
}

sub testPOEStuff {
    my $dummyBackend = PHEDEX::File::Download::Circuits::Backend::Other::Dummy->new(verbose => 1);
    my $session = setupSession($dummyBackend);
    POE::Kernel->run();
}

testPOEStuff();

done_testing;
1;