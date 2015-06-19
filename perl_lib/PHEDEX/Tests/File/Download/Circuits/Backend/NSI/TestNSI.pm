package PHEDEX::Tests::File::Download::Circuits::Backend::NSI::TestNSI;

use strict;
use warnings;

use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::Backend::NSI::NSI;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use POE;
use Switch;
use Test::More;

sub setupSession {
    my $nsiBackend = shift;
    
    my $states;

    $states->{_start} = sub {
        my ($kernel, $session) = @_[KERNEL, SESSION];
        $nsiBackend->Logmsg("Starting a POE test session (id=",$session->ID,")");
        $nsiBackend->_poe_init($kernel, $session);
        
        # Create callbacks
        my $requestCallback = $session->postback("requestCallback");
        
        my $request = PHEDEX::File::Download::Circuits::Backend::Core::ResourceRequest->new( siteA => "Caltech", siteB => "Umich", callback => $requestCallback);
        
        # Delayed request of a circuit
        $kernel->delay('backendRequestResource' => 10, $request);
    };

    $states->{requestCallback} = \&requestCallback;
    $states->{terminateCallback} = \&terminateCallback;

    my $session = POE::Session->create(inline_states => $states);

    return $session;
}

sub requestCallback {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];

    is($status, REQUEST_SUCCEEDED, "TestCore->requestCallback: Request succeeded");
    
    if ($status eq REQUEST_SUCCEEDED) {
        my $terminateCallback = $session->postback("terminateCallback");
        $kernel->delay('backendTeardownResource' => 60, $resource, $terminateCallback);
    }
}

sub terminateCallback {
    my ($kernel, $session, $arguments) = @_[KERNEL, SESSION, ARG1];
    my $resource = $arguments->[0];
    my $status = $arguments->[1];
    is($status, TERMINATE_SUCCEEDED, "TestCore->terminateCallback: terminate succeeded");
}

sub testPOEStuff {
    my $nsiBackend = PHEDEX::File::Download::Circuits::Backend::NSI::NSI->new(verbose => 1);

    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'Caltech', 
                                                                                   netName => 'urn:ogf:network:caltech.edu:2013::CER2024:eth1_24:+?vlan=1790', 
                                                                                   maxBandwidth => 1000);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'Umich', 
                                                                                   netName => 'urn:ogf:network:oess.dcn.umnet.umich.edu:2013::f10-dynes.dcn.umnet.umich.edu:Te+0_1:+?vlan=3179', 
                                                                                   maxBandwidth => 1000);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    $nsiBackend->addPath($path->getName, $path);
    my $session = setupSession($nsiBackend);
    POE::Kernel->run();
}

testPOEStuff();

done_testing;

1;