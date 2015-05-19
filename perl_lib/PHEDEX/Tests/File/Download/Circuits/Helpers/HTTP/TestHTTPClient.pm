package PHEDEX::Tests::File::Download::Circuits::Helpers::HTTP::TestHTTPClient;

use warnings;
use strict;

use JSON::XS;
use POE;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpClient;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpConstants;
use Test::More;

use Sys::Hostname;
use Socket;

# Create master session
POE::Session->create(
        inline_states => {
            _start => sub {
                my ($kernel, $session) = @_[KERNEL, SESSION];

                # Create a user agent and spawn it
                my $userAgent = PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpClient->new();
                $userAgent->spawn();

                # Setup the various tests that we need to do...
                $kernel->post($session, 'runGetMethodTests', $userAgent);
                $kernel->post($session, 'runPostMethodTests', $userAgent);
            },
            stopClient => sub {
                my ($kernel, $session, $userAgent) = @_[KERNEL, SESSION, ARG0];
                $userAgent->unspawn();
            },

            runGetMethodTests   => \&runGetMethodTests,
            runPostMethodTests  => \&runPostMethodTests,

            # Postback used to test replies
            validateHttpRequest => \&validateHttpRequest,
    });

sub runGetMethodTests {
    my ($kernel, $session, $userAgent) = @_[KERNEL, SESSION, ARG0];

    ### Test error handling ##
    # Site doesn't exist
    my $request1 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://this.site.does.not.exist.com/",
                                                                                    callback => $session->postback("validateHttpRequest", undef, 500));
    $userAgent->httpRequest($request1);


    # Site replies with "Found" (usually used to do a URL redirection)
    my $request2 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://www.google.com/",
                                                                                    callback => $session->postback("validateHttpRequest", undef, 302));
    $userAgent->httpRequest($request2);
    
    # Site replies with something, but it's not JSON
    my $request3 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://phys.org/",
                                                                                    callback => $session->postback("validateHttpRequest", undef, 415));
    $userAgent->httpRequest($request3);

    ### Test with sources providing valid json objects ##
    my ($addr) = inet_ntoa((gethostbyname(hostname))[4]);
    my $request4 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://httpbin.org/ip",
                                                                                    callback => $session->postback("validateHttpRequest", { origin => $addr }, 200));
    $userAgent->httpRequest($request4);

    # Test one of the objects that we get back from the server
    my $headers = {
        headers => {
            'Host'          => 'httpbin.org',
            'User-Agent'    => 'POE-Component-Client-HTTP/0.949 (perl; N; POE; en; rv:0.949000)'
        }
    };
    
    my $request5 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://httpbin.org/headers",
                                                                                    callback => $session->postback("validateHttpRequest", $headers, 200));
    $userAgent->httpRequest($request5);

    # Test get method with url encoded data provided
    my $input = { text  => "This text was passed as form data" };
    my $echo = {
        "args" => {
            text => "This text was passed as form data"
        },
        "headers" => {
            "Host"              => "httpbin.org",
            "User-Agent"        => "POE-Component-Client-HTTP/0.949 (perl; N; POE; en; rv:0.949000)"
        },
        "origin"    => "137.138.42.16",
        "url"       => "http://httpbin.org/get?text=This+text+was+passed+as+form+data"
    };

    my $request6 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://httpbin.org/get",
                                                                                    arguments => $input,
                                                                                    callback => $session->postback("validateHttpRequest", $echo, 200));
    $userAgent->httpRequest($request6);
}

sub runPostMethodTests {
    my ($kernel, $session, $userAgent) = @_[KERNEL, SESSION, ARG0];

    # Test data
    my $testData = {
        user            => "vlad",
        password        => "wouldn't you like to know",
        secretQuestion  => "Answer to the Ultimate Question of Life, the Universe, and Everything",
        secretAnswer    => 42
    };

    ### Test error handling ###
    # Site doesn't exist
    my $request1 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://this.site.does.not.exist.com/",
                                                                                    arguments => ["FORM", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", undef, 500));
    $userAgent->httpRequest($request1);
    
    # Method not allowed
    my $request2 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://httpbin.org/get",
                                                                                    arguments => ["FORM", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", undef, 405));
    $userAgent->httpRequest($request2);

    # Test posting of data
    my $request3 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://httpbin.org/post",
                                                                                    arguments => ["FORM", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", $testData, 200, "form"));
    $userAgent->httpRequest($request3);
    
    my $request4 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://httpbin.org/post",
                                                                                    arguments => ["JSON", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", $testData, 200, "json"));
    $userAgent->httpRequest($request4);
    
    my $request5 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://httpbin.org/post",
                                                                                    arguments => ["TEXT", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", undef, 200));
    $userAgent->httpRequest($request5);
    
    my $request6 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://httpbin.org/post",
                                                                                    arguments => ["BLA", $testData],
                                                                                    callback =>$session->postback("validateHttpRequest", undef, HTTP_CLIENT_INVALID_REQUEST));
    $userAgent->httpRequest($request6);
}

sub validateHttpRequest {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];
    my ($expectedObject, $expectedCode, $subObject) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    # In the case of httpbin, we want to check a sub-element of the data which was sent by the server
    $resultObject = $resultObject->{$subObject} if defined $subObject;

    my $uri = defined $resultRequest ? $resultRequest->{"_request"}->{"_uri"} : "unknown";

    my $msg = "TestHTTPClient->validateHttpRequest";
    ok($postArgs, "$msg: ($uri) Arguments defined");

    is_deeply($resultObject, $expectedObject, "$msg: ($uri) Resulted and expected objects matched") if defined $expectedObject;
    is($resultCode, $expectedCode, "$msg: ($uri) Resulted and expected codes matched") if defined $expectedCode;
}


POE::Kernel->run();

done_testing;

1;