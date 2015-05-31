package PHEDEX::File::Download::Circuits::ResourceManager::ResourceManager;

use Moose;

use base 'PHEDEX::Core::Logging', 'Exporter';

use List::Util qw(min);
use Module::Load;
use POE;
use Switch;

use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;

use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Common::Failure;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::Helpers::Utils::UtilsConstants;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpServer;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler;
use PHEDEX::File::Download::Circuits::ManagedResource::Circuit;
use PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::ResourceManager::ResourceManagerConstants;

our @EXPORT = qw(
                RESOURCE_REQUEST_POSSIBLE CIRCUIT_TRANSFERS_FAILED LINK_UNDEFINED RESOURCE_ALREADY_EXISTS RESOURCE_TYPE_UNSUPPORTED LINK_BLACKLISTED LINK_UNSUPPORTED
                );

# ResourceManager only related constants
use constant {
    RESOURCE_REQUEST_POSSIBLE       =>           40,    # Go ahead and request a circuit
    CIRCUIT_TRANSFERS_FAILED        =>          -41,    # This circuit has been blacklisted because too many transfers failed on it
    LINK_UNDEFINED                  =>          -42,    # Provided link is not a valid one
    RESOURCE_ALREADY_EXISTS         =>          -43,    # A resource had already been previously requested for a given link
    RESOURCE_TYPE_UNSUPPORTED       =>          -44,    # Backend does not supported the management of requested resource type
    LINK_BLACKLISTED                =>          -45,    # Temporarily cannot use managed resources on current link
    LINK_UNSUPPORTED                =>          -46,    # Circuits not supported on current link
    LINK_SATURATED                  =>          -47,    # Reached maximum number of circuits supported on current link
};

my $ownHandles = {
    HANDLE_TIMER        =>      'handleTimer',
    REQUEST_CIRCUIT     =>      'requestCircuit',
    REQUEST_BW          =>      'requestBandwidth',
    REQUEST_REPLY       =>      'handleRequestResponse',
    VERIFY_STATE        =>      'verifyStateConsistency',
};

my $backHandles = {
    BACKEND_UPDATE_BANDWIDTH    =>      'backendUpdateResource',
    BACKEND_REQUEST_CIRCUIT     =>      'backendRequestResource',
    BACKEND_TEARDOWN_CIRCUIT    =>      'backendTeardownResource',
};

# TODO: See if we can export this from some common library instead of redefining
# There's MooseX which does that, but adds even more dependencies 
use Moose::Util::TypeConstraints;
    # Enum declaration
    enum 'PoeTimerType',    [qw(Request Blacklist Teardown)];
    
    # Simple subtype declaration
    subtype 'IP', as 'Str', where {&determineAddressType($_) ne ADDRESS_INVALID}, message { "The value you provided is not a valid hostname or IP(v4/v6)"};
no Moose::Util::TypeConstraints;

### Backend related attributes ###
has 'backendType'           => (is  => 'ro', isa => 'Str', required => 1);
has 'backend'               => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::Core::Core');
has 'backendArguments'      => (is  => 'rw', isa => 'Str');

### Resource attributes ###

# Hash of all reasources which are currently online (active circuits)
# Hash of LinkID => ResourcesSet
has 'resourceSets'     => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]',
                           traits  => ['Hash'], 
                           handles => {addResourceSet       => 'set', 
                                       getResourceSet       => 'get',
                                       getResourceSets      => 'values',
                                       resourceSetExists    => 'exists',
                                       deleteResourceSet    => 'delete'});

# Hash of all resources that are pending
# Hash of NetworkResource.Id => NetworkResource
has 'pendingQueue' => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource]',
                       traits  => ['Hash'], 
                       handles => {addPendingResource    => 'set', 
                                   getPendingResource    => 'get',
                                   removePendingResource => 'delete',
                                   pendingResourceExists => 'exists'});

# Hash of all the resources which are currently offline (expired circuits, etc.)
# Hash of LinkID => ResourcesSet
has 'historySets'      => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet]', 
                           traits  => ['Hash'], 
                           handles => {addHistorySet    => 'set', 
                                       getHistorySet    => 'get',
                                       getHistorySets   => 'values',
                                       historySetExists => 'exists', 
                                       deleteHistorySet => 'delete'});

# Queue holding the most recent x resources
# Array of NetworkResource
has 'historyQueue' => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource]',
                                traits  => ['Array'], 
                                handles => {queueOfflineResource    => 'push', 
                                            dequeueOfflineResource  => 'shift', 
                                            offlineQueueSize        => 'count'});

has 'excludedPaths'         => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::Common::Failure]',                   # LinkID => PHEDEX::File::Download::Circuits::Common::Failure 
                                traits  => ['Hash'], 
                                handles => {excludePath         => 'set',
                                            isPathExcluded      => 'exists',
                                            getExcludedReason   => 'get', 
                                            removeExcludedPath  => 'delete'});

has 'maxConcurrentCircuits' => (is  => 'rw', isa => 'Int',  default => 10);                                         # Maximum of number of circuits up at any given time

# Resource history attributes
has 'maxHistorySize'        => (is  => 'rw', isa => 'Int',  default => 1000);                                       # Keep the last xx circuits in memory
has 'maxHourlyFailureRate'  => (is  => 'rw', isa => 'Num',  default => 100);                                        # Maximum file transfers that can fail in one hour

# Timing attributes
has 'blacklistDuration'     => (is  => 'rw', isa => 'Int',  default => HOUR);                                       # Time in seconds, after which a circuit will be reconsidered
has 'periodConsistencyCheck'=> (is  => 'rw', isa => 'Int',  default => MINUTE);                                     # Period for event verify_state_consistency
has 'requestTimeout'        => (is  => 'rw', isa => 'Int',  default => 5 * MINUTE);                                 # If we don't get it by then, we'll most likely not get them at all

# POE related
has 'poeSessionId'          => (is  => 'rw', isa => 'Int');
has 'poeAlarms'             => (is  => 'rw', isa => 'HashRef[HashRef[Str]]');
has 'poeDelays'             => (is  => 'rw', isa => 'HashRef[HashRef[Str]]');

# HTTP related attributes
has 'httpControl'           => (is  => 'rw', isa => 'Bool', default => 0);
has 'httpHandles'           => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandle]', 
                                traits  => ['Array'], 
                                handles => {addHttpHandle => 'push', 
                                            getHttpHandle => 'get'});
has 'httpHostname'          => (is  => 'rw', isa => 'IP',   default => 'localhost');
has 'httpPort'              => (is  => 'rw', isa => 'Int',  default => 8080);
has 'httpServer'            => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpServer'); 

# Miscellaneous attributes
has 'stateDir'              => (is  => 'rw', isa => 'Str',  default => '/tmp/managed');
has 'syncHistoryFolder'     => (is  => 'rw', isa => 'Bool', default => 0);                                          # If this is set, it will also remove circuits from 'offline' folder
has 'verbose'               => (is  => 'rw', isa => 'Bool', default => 0);

# Method called directly after the object is constructed
sub BUILD {
    my $self = shift;

    # Import and create backend
    eval {
        # Import backend at runtime
        my $module = "PHEDEX::File::Download::Circuits::Backend::$self->backendType";
        (my $file = $module) =~ s|::|/|g;
        require $file . '.pm';
        $module->import();

        # Create new backend after import
        $self->backend(new $module($self->backendArguments));
    } or do {
        die "Failed to load/create backend: $@\n"
    };

    if ($self->httpControl) {
        $self->httpServer(PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpServer->new());
        $self->httpServer->startServer($self->httpHostname, $self->httpPort);

        $self->addHttpHandle(new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandle(method => 'POST', uri => '/createCircuit', eventName => 'handleHTTPCircuitCreation', session => $session));
        $self->addHttpHandle(new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandle(method => 'POST', uri => '/removeCircuit', eventName => 'handleHTTPCircuitTeardown', session => $session));
        $self->addHttpHandle(new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandle(method => 'GET', uri => '/getInfo', eventName => 'handleHTTPinfo', session => $session));
    }

}

=pod

# Initialize all POE events (and specifically those related to circuits)

=cut

sub _poe_init
{
    my ($self, $kernel, $session) = @_;
    my $msg = 'ResourceManager->_poe_init';

    # Remembering the session ID for when we need to stop and tear down all the circuits
    $self->poeSessionId($session->ID);

    $self->Logmsg("$msg: Initializing all POE events") if ($self->verbose);

    foreach my $key (keys %{$ownHandles}) {
        $kernel->state($ownHandles->{$key}, $self);
    }

    # Share the session with the circuit booking backend as well
    $self->Logmsg("$msg: Initializing all POE events for backend") if ($self->verbose);
    $self->backend->_poe_init($kernel, $session);

    # Get the periodic events going
    $kernel->yield($ownHandles->{VERIFY_STATE}) if (defined $self->periodConsistencyCheck);

    # Add the handlers for the HTTP events which we want to process
    if (defined $self->httpServer) {
        foreach my $httpHandle (@{$self->httpHandles}) {
            $kernel->state($httpHandle->eventName, $self);
            $self->httpServer->addHandler($httpHandle);
        }
    }
}

# Retrieves all resources from a hash of resource sets
sub getAllResources {
    my ($self, $resourceSets) = @_;
     my $resources;
    foreach my $resourceSet (@{$resourceSets}) {
        foreach my $resource (@{$resourceSet->getAllResources}) {
            $resources->{$resource->id} = $resource;
        }
    }
    return $resources;
}

# This function returns the resource which has the biggest allocated BW 
# so far on the link that was requested.
sub getManagedResource {
    my $msg = "ResourceManager->getManagedResource";
    my ($self, $nodeA, $nodeB) = @_;
    
    if (! defined $nodeA || ! defined $nodeB) {
        $self->Logmsg("$msg: Cannot do anything without a valid imput");
        return undef;
    }

    my $linkID = &getPath($nodeA, $nodeB);

    if ($self->resourceSetExists($linkID)) {
        $self->Logmsg("$msg: The specified link does not exist");
        return undef;
    }

    my $resourceSet = $self->getResourceSet($linkID);
    
    if ($resourceSet->isEmpty) {
        $self->Logmsg("$msg: There are no active resources on the specified link");
        return;
    }

    # Retrieve the resource with the highest allocated BW
    my $resource = $resourceSet->getResourceByBW();

    if ($resource->status eq 'Pending') {
        $self->Logmsg("$msg: Found resources but they are busy with updates");
        return;
    }
    
    return $resource;
}

# TODO: Needs a revision based on workflow
# Method used to check if we can request a certain resource
# - it checks with the backend to see if it supports managing the resource type v
# - it checks with the backend to see if the link supports it
# - it checks if a resource hasn't already been requested/is online
# - it checks if the link isn't currently blacklisted
sub canRequestResource {
    my $msg = "ResourceManager->canRequestResource";
    my ($self, $nodeA, $nodeB) = @_;
    
    if (! defined $nodeA || ! defined $nodeB) {
        $self->Logmsg("$msg: Cannot do anything without valid parameters");
        return;
    }
    
    my $linkName = &getPath($nodeA, $nodeB);
    
    # The path might be blacklisted
    if ($self->isPathExcluded($linkName)) {
        $self->Logmsg("$msg: Link is blacklisted");
        return LINK_BLACKLISTED;
    }
    
    # The link might not even support circuits
    if (!$self->backend->checkLinkSupport($nodeA, $nodeB)) {
        $self->Logmsg("$msg: Cannot request resource on given link");
        return LINK_UNSUPPORTED;
    }
    
    # The link cannot accept any additional requests
    my $resourceSet = $self->getOnlineResource($linkName);
    if (! $resourceSet->canAddResource) {
        $self->Logmsg("$msg: Cannot request additional resources on given link");
        return LINK_SATURATED;
    }
    
    return RESOURCE_REQUEST_POSSIBLE;
}

# This (recurrent) event is used to ensure consistency between data on disk and data in memory
# If the download agent crashed, these are scenarios that we need to check for:
#   internal data is lost, but file(s) exist in :
#   - circuits/requested
#   - circuits/online
#   - circuits/offline
#   - bod/online
#   - bod/offline
sub verifyStateConsistency
{
    my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION];
    my $msg = "ResourceManager->$ownHandles->{VERIFY_STATE}";

    $self->Logmsg("$msg: enter event") if ($self->verbose);
    $self->delay_max($kernel, $ownHandles->{VERIFY_STATE}, $self->periodConsistencyCheck) if (defined $self->periodConsistencyCheck);

    my ($allResources, @circuits, @bod);
    # Read all the folders for each resource type
    &getdir($self->stateDir."/Circuit", \@circuits);
    &getdir($self->stateDir."/Bandwidth", \@bod);
    
    # For each circuit folder, add what you find to the resource hash with the appropiate tag
    foreach my $tag (@circuits) {
        my @circuitsSubset;
        &getdir($self->stateDir."/Circuit/".$tag, \@circuitsSubset);
        $allResources->{'Circuit/'.$tag} = \@circuitsSubset;
    }
    
    # For each bandwidth folder, add what you find to the resource hash with the appropiate tag
    foreach my $tag (@bod) {
        my @bodSubset;
        &getdir($self->stateDir."/Bandwidth/".$tag, \@bodSubset);
        $allResources->{'Bandwidth/'.$tag} = \@bodSubset;
    }
    
    my $timeNow = &mytimeofday();

    foreach my $tag (keys %{$allResources}) {

        # Skip if there are no files in one of the folders
        if (!scalar @{$allResources->{$tag}}) {
            $self->Logmsg("$msg: No files found in /$tag") if ($self->verbose);
            next;
        }

        foreach my $file (@{$allResources->{$tag}}) {
            my $path = $self->stateDir.'/'.$tag.'/'.$file;
            $self->Logmsg("$msg: Now handling $path") if ($self->verbose);

            # Attempt to open resource
            my $resource = &openState($path);

            # Remove the state file if the read didn't return OK
            if (!$resource) {
                $self->Logmsg("$msg: Removing invalid resource file $path");
                unlink $path;
                next;
            }
            
            if ($resource->checkCorrectPlacement($path) == ERROR_GENERIC) {
                $self->Logmsg("$msg: Resource found in incorrect folder. Removing and resaving...");
                unlink $path;
                $resource->saveState();
            }

            my $linkName = $resource->getLinkName;

            # The following three IFs could very well have been condensed into one, but
            # I wanted to provide custom debug messages whenever we skipped them

            # If the backend doesn't match the one we have here, skip it
            if ($resource->bookingBackend ne $self->backendType) {
                $self->Logmsg("$msg: Skipping resource due to different backend used ($resource->bookingBackend vs $self->backendType)") if ($self->verbose);
                next;
            }

            # If the backend no longer supports circuits on those links, skip them as well
            if (! $self->backend->checkLinkSupport($resource->nodeA, $resource->nodeB)) {
                $self->Logmsg("$msg: Skipping resource since the backend no longer supports creation of circuits on $linkName") if ($self->verbose);
                next;
            }

            my $inMemoryResource;

            # Attempt to retrieve the resource if it's in memory
            switch ($tag) {
                case ["Circuit/Offline","Bandwidth/Offline"] {
                    my $offlineResources = $self->getAllResources($self->historySets);
                    $inMemoryResource = $offlineResources->{$resource->id} if defined $offlineResources->{$resource->id};
                }
                else {
                    my $onlineResources = $self->getAllResources($self->resourceSets);
                    $inMemoryResource = $onlineResources->{$resource->id} if defined $onlineResources->{$resource->id};
                }
            };

            # Skip this one if we found an identical circuit in memory
            if (&compareObject($self, $inMemoryResource)) {
                $self->Logmsg("$msg: Skipping identical in-memory resource") if ($self->verbose);
                next;
            }

            # If, for the same link, the info differs between on disk and in memory,
            # yet the scope of the circuit is the same as the one for the CM
            # remove the one on disk and force a resave for the one in memory
            if (defined $inMemoryResource) {
                 $self->Logmsg("$msg: Removing similar circuit on disk and forcing resave of the one in memory");
                 unlink $path;
                 $inMemoryResource->saveState();
                 next;
            }

            # If we get to here it means that we didn't find anything in memory pertaining to a given link
            
            switch ($tag) {
                case 'Circuit/Pending' {
                    # This is a bit tricky to handle.
                    #   1) The circuit could still be 'in request'. If the booking agent died as well
                    #      then the circuit could be created and not know about it :|
                    #   2) The circuit might be online by now
                    # What we do now is flag the circuit as offline, then have it in the offline thing for historical purposes
                    # TODO : Another solution would be to wait a bit of time then attempt to 'teardown' the circuit
                    # This would ensure that everything is 'clean' after this method
                    unlink $path;
                    $resource->registerRequestFailure('Failure to restore request from disk');
                    $resource->saveState();
                }
                case 'Circuit/Online' {
                    # Skip circuit if the link is currently blacklisted
                    if ($self->isPathExcluded($linkName)) {
                        $self->Logmsg("$msg: Skipping circuit since $linkName is currently blacklisted");
                        # We're not going to remove the file. It might be useful once the blacklist timer expires
                        next;
                    }

                    # Now there are two cases:
                    if ($resource->isExpired()) {

                        # Use the circuit
                        $self->Logmsg("$msg: Found established circuit $linkName. Using it");
                        
                        $self->{RESOURCES}{$linkName} = $resource;

                        if (defined $resource->lifetime) {
                            my $delay = $resource->getExpirationTime() - &mytimeofday();
                            next if $delay < 0;
                            $self->Logmsg("$msg: Established circuit has lifetime defined. Starting timer for $delay");
                            $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $delay, TIMER_TEARDOWN, $resource);
                        }

                    } else {                                                                                                        # Else we attempt to tear it down
                        $self->Logmsg("$msg: Attempting to teardown expired circuit $linkName");
                        $self->handleCircuitTeardown($kernel, $session, $resource);
                    }
                }
                case ['circuits/offline', 'bod/offline'] {
                    # Don't add the circuit if the history is full and circuit is older than the oldest that we currently have on record
                    my $oldestCircuit = $self->{RESOURCE_HISTORY_QUEUE}->[0];
                    if (scalar @{$self->{RESOURCE_HISTORY_QUEUE}} < $self->maxHistorySize ||
                        !defined $oldestCircuit || $resource->lastStatusChange > $oldestCircuit->lastStatusChange) {
                        $self->Logmsg("$msg: Found offline circuit. Adding it to history");
                        $self->addResourceToHistory($resource);
                    }
                }
            }
        }
    }
}

# Adds a resource to online set
sub addOnlineResource {
    my ($self, $resource) = @_;
    my $msg = "ResourceManager->addOnlineResource";

    if (! defined $resource) {
        $self->Logmsg("$msg: Invalid resource provided");
        return undef;
    }

    my $linkName = $resource->getLinkName;
    my $onlineSet;
    
    # Check if a set already exists for this link; create it if not
    if (! $self->resourceSetExists($linkName)) {
        $self->Logmsg("$msg: First time adding a resource for this particular link ($linkName)"); 
        $onlineSet = PHEDEX::File::Download::Circuits::ResourceManager::ResourceSet->new('maxResources' => $self->maxHistorySize);
    } else {
        $onlineSet = $self->getResourceSet($linkName);
    }

    # Try and add the resource to the set
    my $result = $onlineSet->addResource($resource);
    $self->Logmsg("$msg: Could not add resource") if (! defined $result);
    return $result;
}

# Adds a resource to history
sub addResourceToHistory {
    my ($self, $resource) = @_;

    my $msg = "ResourceManager->addResourceToHistory";

    if (! defined $resource) {
        $self->Logmsg("$msg: Invalid resource provided");
        return;
    }
    
    # Remove oldest circuit from history 
    if ($self->offlineQueueSize >= $self->maxHistorySize) {
        eval {
            my $oldestResource = $self->dequeueOfflineResource; 
            $self->Logmsg("$msg: Removing oldest resource from history ($oldestResource->id)") if $self->verbose;
            my $resourceSet = $self->getHistorySet($oldestResource->getLinkName());
            $resourceSet->deleteResource($oldestResource->id);
            $oldestResource->removeState() if ($self->syncHistoryFolder);
        }
    }

    $self->Logmsg("$msg: Adding resource ($resource->id) to history");

    # Add new resource to history
    eval {
        $self->queueOfflineResource($resource);
        my $linkName = $resource->getLinkName();
        if (! $self->historySetExists($resource)) {
            my $offlineSet = PHEDEX::File::Download::Circuits::ResourceManager::ResourceSet->new('maxResources' => $self->maxHistorySize);
            $self->addOfflineSet($offlineSet);
        }
        my $resourceSet = $self->getHistorySet($linkName);
        $resourceSet->addResource($resource);
    }
}

sub retireOnlineResource {
    my ($self, $resource) = @_;
    my $msg = "ResourceManager->retireOnlineResource";

    if (! defined $resource) {
        $self->Logmsg("$msg: Invalid resource provided");
        return undef;
    }

    # First check to see if the resource is really online
    my $linkName = $resource->getLinkName;
    my $onlineSet = $self->getResourceSet($linkName);
    if (! defined $onlineSet || ! $onlineSet->resourceExists($resource)) {
        $self->Logmsg("$msg: Cannot retire a resource which is not online");
        return undef;
    }
    
    # Remove the resource from the online set
    $onlineSet->deleteResource($resource);
    $self->deleteOnlineSet($linkName) if $onlineSet->isEmpty;
    
    # Add the resource to the history set
    return $resource;
}


# Blacklists a link and starts a timer to unblacklist it after BLACKLIST_DURATION
sub addLinkToBlacklist {
    my ($self, $resource, $failure, $delay) = @_;

    my $msg = "ResourceManager->addLinkToBlacklist";

    if (! defined $resource) {
        $self->Logmsg("$msg: Invalid resource provided");
        return;
    }

    my $linkName = $resource->name;
    $delay = $self->blacklistDuration if ! defined $delay;

    $self->Logmsg("$msg: Adding link ($linkName) to history. It will be removed after $delay seconds");

    $self->excludePath($linkName, $failure);
    $self->delayAdd($poe_kernel, $ownHandles->{HANDLE_TIMER}, $delay, TIMER_BLACKLIST, $resource);
}

# This routine is called by the CircuitAgent when a transfer fails
# If too many transfer fail, it will teardown and blacklist the circuit
sub transferFailed {
    my ($self, $resource, $task) = @_;

    my $msg = "ResourceManager->transferFailed";

    if (!defined $resource || !defined $task) {
        $self->Logmsg("$msg: Invalid parameters");
        return;
    }

    if ($resource->status != 'Online') {
        $self->Logmsg("$msg: Can't do anything with this resource");
        return;
    }

    # Tell the circuit that a transfer failed on it
    my $failure = $resource->registerTransferFailure($task);

    my $transferFailures = $resource->getTransferFailureCount;
    my $lastHourFails;
    my $now = &mytimeofday();

    foreach my $fails (@{$transferFailures}) {
        $lastHourFails++ if ($fails->[0] > $now - HOUR);
    }

    my $linkName = $resource->getLinkName;

    if ($lastHourFails > $self->maxHourlyFailureRate) {
        $self->Logmsg("$msg: Blacklisting $linkName due to too many transfer failures");

        # Blacklist the circuit
        my $failure = 
        $self->addLinkToBlacklist($resource, $failure);

        # Tear it down
        $self->handleCircuitTeardown($poe_kernel, $poe_kernel->ID_id_to_session($self->poeSessionId), $resource);
    }
}

sub requestBandwidth {
    my ( $self, $kernel, $session, $nodeA, $nodeB, $bandwidth) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2 ];

    my $msg = "ResourceManager->$ownHandles->{REQUEST_BW}";

    my $linkName = &getPath($nodeA, $nodeB);

    return if !defined $linkName;

    my $resource;

    # Check if a bandwidth is not already provisioned
    if (defined $self->{RESOURCES}{$linkName}) {
        $resource = $self->{RESOURCES}{$linkName};      
    } else {
        $resource = PHEDEX::File::Download::Circuits::ManagedResource::Bandwidth->new(STATE_DIR => $self->stateDir,
                                                                                      SCOPE => $self->{SCOPE},
                                                                                      VERBOSE => $self->verbose);
        $resource->initResource($self->backendType, $nodeA, $nodeB, 1);
    }

    # Switch from the 'offline' to 'requesting' state, save to disk and store this in our memory
    eval {
        $resource->registerUpdateRequest($bandwidth, 1);
        $self->{RESOURCES}{$linkName} = $bandwidth;
        $resource->saveState();

        # Start the watchdog in case the request times out
        $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $resource->{REQUEST_TIMEOUT}, TIMER_REQUEST, $resource);
        $kernel->post($session, $backHandles->{BACKEND_UPDATE_BANDWIDTH}, $resource, $ownHandles->{REQUEST_REPLY});
    };
}

sub requestCircuit {
    my ( $self, $kernel, $session, $nodeA, $nodeB, $lifetime, $bandwidth) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2, ARG3 ];

    my $msg = "ResourceManager->$ownHandles->{REQUEST_CIRCUIT}";

    my $linkName = &getPath($nodeA, $nodeB);
    
    if ($self->canRequestResource($nodeA, $nodeB) != RESOURCE_REQUEST_POSSIBLE) {
        $self->Logmsg("$msg: Cannot request resource ATM");
        return;
    }

    $self->Logmsg("$msg: Attempting to request a circuit for link $linkName");
    defined $lifetime ? $self->Logmsg("$msg: Lifetime for link $linkName is $lifetime seconds") :
                        $self->Logmsg("$msg: Lifetime for link $linkName is the maximum allowable by IDC");

    # Create the circuit object
    
    my $circuit = PHEDEX::File::Download::Circuits::ManagedResource::Circuit->new(bookingBackend    => $self->backendType,
                                                                                  nodeA             => $nodeA, 
                                                                                  nodeB             => $nodeB,
                                                                                  requestTimeout    => $self->requestTimeout,
                                                                                  stateDir          => $self->stateDir,
                                                                                  verbose           => $self->verbose
                                                                                  );

    $self->Logmsg("$msg: Created circuit in request state for link $linkName (Circuit ID = $circuit->id)");

    # Switch from the 'offline' to 'requesting' state, save to disk and store this in our memory
    eval {
        $circuit->registerRequest($lifetime, $bandwidth);
        my $resourceSet;
        
        if ($self->resourceSetExists($linkName)) {
            $resourceSet = $self->getResourceSet($linkName);
        } else {
            $resourceSet= PHEDEX::File::Download::Circuits::ResourceManager::ResourceSet->new(maxResources => $self->maxHistorySize);
            $self->addOnlineSet($resourceSet);
        }

        $circuit->saveState();

        # Start the watchdog in case the request times out
        $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $circuit->requestTimeout, TIMER_REQUEST, $circuit);

        $kernel->post($session, $backHandles->{BACKEND_REQUEST_CIRCUIT}, $circuit, $ownHandles->{REQUEST_REPLY});
    };

}

# This method is called when a circuit request fails.
# This is either because the request itself failed (got a reply and an error code) or
# the request timed out. In either case, this is obviously bad and what needs doing
# is the same in both cases
sub handleRequestFailure {
    my ($self, $resource, $code) = @_;

    my $msg = "ResourceManager->eventNameequestFailure";

    if (!defined $resource) {
        $self->Logmsg("$msg: No circuit was provided");
        return;
    }

    my $linkName = $resource->getLinkName;
    my $resourceSet = $self->getOnlineResource($linkName);
    if (! $resourceSet) {
        $self->Logmsg("$msg: There are no online sets matching the resource attributes");
        return;
    }
    
    if ($resource->status eq 'Pending') {
        $self->Logmsg("$msg: Can't do anything with this resource");
        return;
    }

    # We got a response for the request - we need to remove the timer set in case the request timed out
    $self->delayRemove($poe_kernel, TIMER_REQUEST, $resource);

    eval {
        $self->Logmsg("$msg: Updating internal data");
        # Remove the state that was saved to disk
        $resource->removeState();
        
        # Update circuit object internal data as well
        switch($resource->resourceType) {
            case 'Circuit' {
                # Remove from hash of all circuits, then add it to the historical list
                $resourceSet->deleteResource($resource->id);
                $self->addResourceToHistory($resource);
                $resource->registerRequestFailure($code);
            }
            case 'Bandwidth'{
                $resource->registerUpdateFailed();
            }
        }

        # Update circuit object internal data as well
        
        $resource->saveState();

        # Blacklist this link
        # This needs to be done *after* we register the failure with the circuit
        $self->addLinkToBlacklist($resource, CIRCUIT_REQUEST_FAILED);
    }
}

sub handleRequestResponse {
    my ($self, $kernel, $session, $resource, $returnValues, $code) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2 ];

    my $msg = "ResourceManager->$ownHandles->{REQUEST_REPLY}";

    if (! defined $resource || ! defined $code) {
        $self->Logmsg("$msg: Resource or code not defined");
        return;
    }

    my $linkName = $resource->getLinkName();
    
    # TODO: Code potentiall removable
    if (($resource->resourceType eq 'Circuit' && $resource->status eq 'Pending'))	 {
        $self->Logmsg("$msg: Can't do anything with this resource");
        return;
    }
        
    # If the request failed, call the method handling request failures
    if ($code < 0) {
        $self->Logmsg("$msg: Circuit request failed for $linkName");
        $self->eventNameequestFailure($resource, $code);
        return;
    }

    $self->Logmsg("$msg: Request succeeded for $linkName");
    
    # We got a response for the request - we need to remove the timer set in case the request timed out
    $self->delayRemove($kernel, TIMER_REQUEST, $resource);
    
    # Erase old state	
    $resource->removeState();
         
    # Update state
    switch($resource->resourceType) {
        case 'Circuit' {
            $resource->registerEstablished($returnValues->{IP_A}, $returnValues->{IP_B}, $returnValues->{BANDWIDTH});
        }
        case 'Bandwidth' {
            $resource->registerUpdateSuccessful();
        }
    }
    
    # Save new state
    $resource->saveState();
    
    $self->Logmsg("$msg: Circuit has an expiration date. Starting countdown to teardown");
    $self->delayAdd($poe_kernel, $ownHandles->{HANDLE_TIMER}, $resource->lifetime, TIMER_TEARDOWN, $resource);
}

sub handleTimer {
    my ($self, $kernel, $session, $timerType, $circuit) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];

    my $msg = "ResourceManager->$ownHandles->{HANDLE_TIMER}";

    if (!defined $timerType || !defined $circuit) {
        $self->Logmsg("$msg: Don't know how to handle this timer");
        return;
    }

    my $linkName = $circuit->getLinkName();

    switch ($timerType) {
        case TIMER_REQUEST {
            $self->Logmsg("$msg: Timer for circuit request on link ($linkName) has expired");
            $self->handleRequestFailure($circuit, CIRCUIT_REQUEST_FAILED_TIMEDOUT);
        }
        case TIMER_BLACKLIST {
            $self->Logmsg("$msg: Timer for blacklisted link ($linkName) has expired");
            $self->handleTrimBlacklist($circuit);
        }
        case TIMER_TEARDOWN {
            $self->Logmsg("$msg: Life for circuit ($circuit->{ID}) has expired");
            $self->handleCircuitTeardown($kernel, $session, $circuit);
        }
    }
}

# Circuits can be blacklisted for two reasons
# 1. A circuit request previously failed
#   - to prevent successive multiple retries to the same IDC, we temporarily blacklist that particular link
# 2. Multiple files in a job failed while being transferred on the circuit
#   - if transfers fail because of a circuit error, by default PhEDEx will retry transfers on the same link
#   we temporarily blacklist that particular link and PhEDEx will retry on a "standard" link instead
sub handleTrimBlacklist {
    my ($self, $circuit) = @_;
    return if ! defined $circuit;
    my $linkName = $circuit->getLinkName();
    if (! $self->isPathExcluded($linkName)) {
        $self->Logmsg("ResourceManager->handleTrimBlacklist: Cannot whitelist a path which is not blacklisted");
        return;
    }
    
    $self->Logmsg("ResourceManager->handleTrimBlacklist: Removing $linkName from blacklist");
    $self->removeExcludedPath($linkName) if $self->isPathExcluded($linkName);
    $self->delayRemove($poe_kernel, TIMER_BLACKLIST, $circuit);
}

sub handleCircuitTeardown {
    my ($self, $kernel, $session, $resource) = @_;

    my $msg = "ResourceManager->handleCircuitTeardown";

    if (!defined $resource) {
        $self->Logmsg("$msg: Invalid parameters received");
        return;
    }
    
    my $linkName = $resource->getLinkName();
    my $resourceSet = $self->getResourceSet($linkName);
    
    if (! $self->resourceSetExists($linkName) || ! $resourceSet->resourceExists($resource)) {
        $self->Logmsg("$msg: The specified resource doesn't seem to be online");
        return;
    }
    
    $self->delayRemove($kernel, TIMER_TEARDOWN, $resource);
    $self->Logmsg("$msg: Updating states for link $linkName");

    eval {
        $resource->removeState();

        # Remove from hash of all circuits, then add it to the historical list
        $resourceSet->deleteResource($resource->id);
        $self->addResourceToHistory($resource);

        # Update circuit object data
        $resource->registerTakeDown();

        # Potentiall save the new state for debug purposes
        $resource->saveState();
    };

    $self->Logmsg("$msg: Calling teardown for $linkName");

    # Call backend to take down this circuit
    $kernel->post($session, $backHandles->{BACKEND_TEARDOWN_CIRCUIT}, $resource);
}

## HTTP Related controls

sub handleHTTPCircuitCreation {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($circuitManager) = @{$initialArgs};
    my ($resultArguments) = @{$postArgs};
    
    my $fromNode = $resultArguments->{NODE_A};
    my $toNode = $resultArguments->{NODE_B};
    my $lifetime = $resultArguments->{LIFETIME};
    my $bandwidth = $resultArguments->{BANDWIDTH};
    
    $circuitManager->Logmsg("Received circuit creation request for nodes $fromNode and $toNode");
    
    $poe_kernel->post($session, 'requestCircuit', $fromNode, $toNode, $lifetime, $bandwidth);
}

sub handleHTTPCircuitTeardown {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($circuitManager) = @{$initialArgs};
    my ($resultArguments) = @{$postArgs};
    
    my $resourceId = $resultArguments->{RESOURCE_ID};
    my $resource = $circuitManager->getResourceSets->{$resourceId};
    
    if (! defined $resource) {
        $circuitManager->Logmsg("Cannot find any circuit to teardown with the specified ID");
        return;
    }

    $circuitManager->Logmsg("Received circuit teardown request for circuit $resourceId");
    $circuitManager->handleCircuitTeardown($kernel, $session, $resource);
}


sub handleHTTPinfo {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($circuitManager) = @{$initialArgs};
    my ($resultArguments, $resultCallback) = @{$postArgs};

    my $request = $resultArguments->{REQUEST};

    return if ! defined $request;

    switch($request) {
        case /^(resourceSets|historySets|pendingQueue|backendType|excludedPaths)$/ {
            $resultCallback->($circuitManager->{$request});
        }
        case 'onlineResource' {
            my $resourceId = $resultArguments->{RESOURCE_ID};
            
            fdsafsd
            
            my $resource = $circuitManager->getResourceSets->{$resourceId};
            $resultCallback->($resource);
        }
        else {
            $resultCallback->();
        }
    }
}

sub stop {
    my $self = shift;

    # Tear down all circuits before shutting down
    $self->teardownAll();

    # Stop the HTTP server
    if (defined $self->httpServer) {
        $self->httpServer->stopServer();
        $self->httpServer->clearHandlers();
    }
}

# Cancels all requests in progress and tears down all the circuits that have been established
sub teardownAll {
    my $self = shift;

    my $msg = "ResourceManager->teardownAll";
    $self->Logmsg("$msg: Cleaning out all circuits");

    my $resources = $self->getResourceSets;
    
    foreach my $resource (values %{$resources}) {
        my $backend = $self->backend;
        switch ($resource->status) {
            case 'Pending' {
                # TODO: Check and see if you can cancel requests
                # $backend->cancel_request($circuit);
            }
            case 'Online' {
                $self->Logmsg("$msg: Tearing down resource $resource->id");
                $self->handleCircuitTeardown($poe_kernel, $poe_kernel->ID_id_to_session($self->poeSessionId), $resource);
            }
        }
    }
}


# Adds a delay and keep the alarm ID which is returned in memory
# It has basically the same effect as delay_add, however, by having the
# alarm ID, we can cancel the timer if we know we don't need it anymore
# for ex. cancel the request time out when we get a reply, or
# cancel the lifetime timer, if we need to destroy the circuit prematurely

# Depending on the architecture each tick of a delay adds about 10ms of overhead
sub delayAdd {
    my ($self, $kernel, $handle, $timer, $timerType, $resource) = @_;

    # Set a delay for a given event, then get the ID of this timer
    my $eventID = $kernel->delay_set($handle, $timer, $timerType, $resource);

    # Remember this ID in order to clean it immediately after which we recevied an answer
    $self->poeDelays->{$timerType}{$resource->id} = $eventID;
}

# Remove an alarm/delay before the trigger time
sub delayRemove {
    my ($self, $kernel, $timerType, $resource) = @_;

    # Get the ID for the specified timer
    my $eventID = $self->poeDelays->{$timerType}{$resource->id};

    # Remove from ResourceManager and remove from POE
    delete $self->poeDelays->{$timerType}{$resource->id};
    $kernel->alarm_remove($eventID);
}

# schedule $event to occur AT MOST $maxdelta seconds into the future.
# if the event is already scheduled to arrive before that time,
# nothing is done.  returns the timestamp of the next event
sub delay_max
{
    my ($self, $kernel, $event, $maxdelta) = @_;
    my $now = &mytimeofday();
    my $id = $self->poeAlarms->{$event}->{ID};
    my $next = $kernel->alarm_adjust($id, 0);
    if (!$next) {
        $next = $now + $maxdelta;
        $id = $kernel->alarm_set($event, $next);
    } elsif ($next - $now > $maxdelta) {
        $next = $kernel->alarm_adjust($id, $now - $next + $maxdelta);
    }
    $self->poeAlarms->{$event} = { ID => $id, NEXT => $next };
    return $next;
}

1;