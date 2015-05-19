package PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpClient;

use Moose;

use base 'PHEDEX::Core::Logging';

use JSON::XS;
use HTTP::Request;
use HTTP::Request::Common;
use HTTP::Status qw(:constants);
use POE::Component::Client::HTTP;
use POE;
use Switch;

use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpConstants;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest;

has 'uaAlias'   => (is  => 'rw', isa => 'Str', default => 'poeHttpClient');
has 'uaTimeout' => (is  => 'rw', isa => 'Int', default => 5);
has 'spawned'   => (is  => 'rw', isa => 'Bool', default => 0);

# Starts the client
# The client will keep connections alive for more 15 seconds, in case multiple requests will be made to the same address
sub spawn {
    my $self = shift;

    my $msg = "HttpClient->spawn";
    if ($self->spawned) {
        $self->Logmsg("$msg: Cannot (won't) spawn another instance of the HTTP Client. Please use the current one...");
        return;
    }

    # Create a user agent which will be referred as "poeHttpClient" (default).
    POE::Component::Client::HTTP->spawn(
        Alias   => $self->uaAlias,
        Timeout => $self->uaTimeout,
    );

    $self->spawned(1);
}

# Calls the client's 'shutdown' state which in turn, responds to all pending requests with
# 408 (request timeout), and then shuts down the component and all subcomponents
sub unspawn {
    my $self = shift;

    my $msg = "HttpClient->spawn";
    if (! $self->spawned) {
        $self->Logmsg("$msg: There is no HTTP Client which is currently spawned");
        return;
    }

    $self->Logmsg("$msg: Unspawning client");
    $poe_kernel->post($self->uaAlias, "shutdown");
     $self->spawned(0)
}

# HTTP GET: only used to retrieve data from the URL. Arguments can be specified
# These arguments however need to be specified as hashes
sub httpRequest {
    my ($self, $request) = @_;

    my $msg = "HttpClient->httpRequest";

    if (! $self->spawned) {
        $self->Logmsg("$msg: There is no HTTP Client which is currently spawned");
        return HTTP_CLIENT_NOT_SPAWNED;
    }

    if (!defined $request) {
        $self->Logmsg("$msg: Invalid parameters were specified");
        return HTTP_CLIENT_INVALID_PARAMS;
    }

    # Create a session for this request (one session = one request)
    POE::Session->create(
        inline_states => {
            _start => sub {
                my ($kernel, $session) = @_[KERNEL, SESSION];
                my $arguments = $request->arguments;
                switch($request->method) {
                    case "GET" {
                        if (defined $request->arguments && ref $request->arguments ne ref {}) {
                             $self->Logmsg("$msg: Arguments were specified, but we need them in hash form");
                            return HTTP_CLIENT_INVALID_PARAMS;
                        }
                        $kernel->post($session, "httpGetRequest", $request->arguments);
                    }
                    case "POST" {
                        if (ref $request->arguments ne ref []) {
                             $self->Logmsg("$msg: Arguments were specified, but we need them in array form");
                            return HTTP_CLIENT_INVALID_PARAMS;
                        }
                        my ($contentType, $content) = @{$request->arguments};
                        $kernel->post($session, "httpPostRequest", $contentType, $content);
                    }
                    else {
                        $self->Logmsg("$msg: Other requests types are unsupported for now");
                        return HTTP_CLIENT_INVALID_REQUEST;
                    }
                }
            },

            httpGetRequest  => sub {
                my ($kernel, $arguments) = @_[KERNEL, ARG0];

                # GET method, so we need to encode the arguments in the URL itself
                my $url = $request->url;
                my $urlEncoded = URI->new($url);
                $urlEncoded->query_form($arguments) if defined $arguments;

                # Create HTTP GET request with arguments in form data
                my $uaRequest = HTTP::Request->new(GET => $urlEncoded);

                # Submit request
                $kernel->post($self->uaAlias, "request", "gotResponse", $uaRequest);
            },

            httpPostRequest => sub {
                my ($kernel, $contentType, $content) = @_[KERNEL, ARG0, ARG1];

                my $uaRequest;
                my $url = $request->url;
                switch($contentType) {
                    case 'FORM' {
                        if (ref $content ne ref {}) {
                             $self->Logmsg("$msg: We need a hash ref when sending FORM encoded data");
                            return;
                        }
                        $uaRequest = POST "$url", $content;
                    }
                    case 'JSON' {
                        $uaRequest = HTTP::Request->new(POST => $url);
                        $uaRequest->header('content-type' => 'application/json');
                        my $jsonObject = JSON::XS->new->convert_blessed->encode($content);
                        $uaRequest->content($jsonObject);
                    }
                    case 'TEXT' {
                        $uaRequest = HTTP::Request->new(POST => $url);
                        $uaRequest->header('content-type' => 'text/html');
                        $uaRequest->content($content);
                    }
                    else {
                        $self->Logmsg("$msg: Don't know how to encode the data that you want to send as the content type that you specified ($contentType)");
                        $request->callback->(undef, HTTP_CLIENT_INVALID_REQUEST, undef);
                        return;
                    }
                }

                $kernel->post($self->uaAlias => request => gotResponse => $uaRequest);
            },

            gotResponse => sub {
                my ($heap, $uaRequest, $response) = @_[HEAP, ARG0, ARG1];

                my $httpResponse = $response->[0];
                my $code = $httpResponse->code();

                if (!$self->replyOk($httpResponse)) {
                    $request->callback->(undef, $code, $httpResponse);
                    return;
                };

                my $contentTypes = $httpResponse->headers()->{'content-type'};

                # TODO: Allow for the server to send OKs in something other than JSON after requests via POST
                if ($contentTypes !~ 'application/json') {
                    $self->Logmsg("$msg: We received a valid response, but we cannot process its content ($contentTypes). We currently only support 'application/json'");
                    $request->callback->(undef, HTTP_UNSUPPORTED_MEDIA_TYPE, $httpResponse);
                    return;
                }

                my $json_content = $httpResponse->decoded_content;

                # TODO: Need to validate content before attempting to decode as json...
                my $decoded_json = decode_json($json_content);

                $request->callback->($decoded_json, $code, $httpResponse);
            }
        }
    );
}

sub replyOk {
    my ($self, $httpResponse) = @_;

    # Check to see if request was successfull
    if (! $httpResponse->is_success) {
        my $code = $httpResponse->code;
        my $message = $httpResponse->message;
        $self->Logmsg("HttpClient->replyOk: an error has occured (CODE: $code, MESSAGE: $message)");
        return 0;
    }

    return 1;
}

1;