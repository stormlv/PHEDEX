package PHEDEX::Tests::File::Download::Circuits::ManagedResource::TestNetworkResource;

use strict;
use warnings;

use IO::File;
use Test::More;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::ManagedResource::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Path;
use PHEDEX::File::Download::Circuits::ResourceManager::ResourceManagerConstants;

# Test the "getPath" function - used to return the link name 
sub testHelperMethods {
    my $msg = "TestNetworkResource->testHelperMethods";
    
    my $wrongPath1 = getPath(undef, "NodeB");
    is($wrongPath1, undef, "$msg: Get path cannot return path with one of the nodes undef");
    my $wrongPath2 = getPath("NodeA", undef);
    is($wrongPath2, undef, "$msg: Get path cannot return path with one of the nodes undef");
    
    my $path1 = getPath("NodeA", "NodeB", 0);
    is($path1, "NodeA-to-NodeB", "$msg: Get path correctly returns path for bidirectional path");
    my $path2 = getPath("NodeA", "NodeB", 1);
    is($path2, "NodeA-NodeB", "$msg: Get path correctly returns path for unidirectional path");
    my $path3 = getPath("NodeA", "NodeB", undef);
    is($path3, "NodeA-to-NodeB", "$msg: Get path correctly returns path with default values");
}

# Self explaining test
sub testInitialisation {
    my $msg = "TestNetworkResource->testInitialisation";
    
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Node->new(siteName => 'NodeA', endpointName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Node->new(siteName => 'NodeB', endpointName => 'STP2', maxBandwidth => 222);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');

    my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendType => 'Dummy',
                                                                                           resourceType    => 'Circuit',
                                                                                           path => $path);
    
    # Provide all the correct parameters to the initialisation and test to see if they were all set in the object
    is($resource->backendType, "Dummy", "$msg: Initialisation ok (backend matches)");
    is($resource->resourceType, "Circuit", "$msg: Initialisation ok (type matches)");
    is($resource->path->getSiteNameA, "NodeA", "$msg: Initialisation ok (NodeB matches)");
    is($resource->path->getSiteNameB, "NodeB", "$msg: Initialisation ok (NodeB matches)");
    is($resource->scope, "Generic", "$msg: Initialisation ok (scope matches)");
    ok($resource->lastStatusChange, "$msg: Initialisation ok (remembered last status change)");
}

# Trivial test consisting of trying to open invalid circuits
sub testOpenErrorHandling {
    my $msg = "TestNetworkResource->testOpenErrorHandling";

    is(PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource::openState(undef), ERROR_PARAMETER_UNDEF, "$msg: Unable to open since path is not defined");
    
    my $fileLocation = "/tmp/tests/bla.resource";
    is(PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource::openState($fileLocation), ERROR_FILE_NOT_FOUND, "$msg: Unable to open since the path is invalid");
    
    # Create a "bad"/"malformed" resource
    my $fh = new IO::File->new($fileLocation, 'w');
    if (defined $fh) {
        print $fh "This is malformed file\n";
        $fh->close();
    }

    is(PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource::openState($fileLocation), ERROR_INVALID_OBJECT, "$msg: Unable to open an invalid resource");
    
    unlink $fileLocation;
}

testHelperMethods();
testInitialisation();
testOpenErrorHandling();

done_testing();

1;
