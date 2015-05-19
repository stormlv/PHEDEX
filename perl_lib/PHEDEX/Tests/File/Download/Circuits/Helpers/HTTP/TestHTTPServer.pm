package PHEDEX::Tests::File::Download::Circuits::Helpers::HTTP::TestHTTPServer;

use warnings;
use strict;

# Normal imports
use POE;
use Test::More;

# PhEDEx imports
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpClient;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest;
use PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpServer;

# Create master session
POE::Session->create(
        inline_states => {
            _start => sub {
                my ($kernel, $session) = @_[KERNEL, SESSION];

                # Create a http client and spawn it. It has been tested independently of this server
                # so we should be ok with using it these tests
                my $httpClient = PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpClient->new();
                $httpClient->spawn();

                # Setup the various tests that we need to do...
                $kernel->post($session, 'runGetMethodTests', $httpClient);
            },
            stopClient => sub {
                my ($kernel, $session, $httpClient, $httpServer) = @_[KERNEL, SESSION, ARG0, ARG1];

                # Stop the http client
                $httpClient->unspawn();

                # Stop the http server
                $httpServer->stopServer();
                $httpServer->clearHandlers();
            },

            runGetMethodTests   => \&runGetMethodTests,

            # Postback for the http client. It is called after it receives data from the server.
            # We use it to test the data that we get back
            httpClientPostback  => \&httpClientPostback,

            # This postback is created by a client which wants handle a given URL (/, /example, etc.)
            # It is linked to that URL via the addHandler method. This tells the HTTP Server that that
            # postback is to be called for a given combination of HTTP Request and URL (GET, "/")
            postbackForGetHandler  => \&postbackForGetHandler,
            postbackForPostHandler => \&postbackForPostHandler,
    });

sub runGetMethodTests {
    my ($kernel, $session, $httpClient) = @_[KERNEL, SESSION, ARG0];

    # Create the server
    my $httpServer = PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpServer->new();
    $httpServer->startServer("localhost", 8080);

    # Test data
    my $testData = {
        user            => "vlad",
        password        => "wouldn't you like to know",
        secretQuestion  => "Answer to the Ultimate Question of Life, the Universe, and Everything",
        secretAnswer    => 42
    };

    my $handler1 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler(method => 'GET', 
                                                                                   uri => '/nodata', 
                                                                                   eventName => 'postbackForGetHandler', 
                                                                                   session => $session);
    $httpServer->addHandler($handler1->id, $handler1);

    my $handler2 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler(method => 'GET', 
                                                                                   uri => '/', 
                                                                                   eventName => 'postbackForGetHandler', 
                                                                                   session => $session, 
                                                                                   arguments => [$testData]);
    $httpServer->addHandler($handler2->id, $handler2);

    my $handler3 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler(method => 'GET', 
                                                                                   uri => '/args', 
                                                                                   eventName => 'postbackForGetHandler', 
                                                                                   session => $session, 
                                                                                   arguments => [$testData, {'test' => 'result'}]);
    $httpServer->addHandler($handler3->id, $handler3);

    my $handler4 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpHandler(method => 'POST', 
                                                                                   uri => '/post', 
                                                                                   eventName => 'postbackForPostHandler', 
                                                                                   session => $session, 
                                                                                   arguments => [$testData]);
    $httpServer->addHandler($handler4->id, $handler4);

    ### Test error handling ###
    
    # Handler supplies an invalid object
    my $request1 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://localhost:8080/nodata", 
                                                                                    callback => $session->postback("httpClientPostback", undef, 500));
    $httpClient->httpRequest($request1);
    
    # Handler doesn't exist for this method
    my $request2 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://localhost:8080/nodata", 
                                                                                    callback => $session->postback("httpClientPostback", undef, 400));
    $httpClient->httpRequest($request2);
    
    # Handler doesn't exist for this URI
    my $request3 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://localhost:8080/invalidmethod", 
                                                                                    callback => $session->postback("httpClientPostback", undef, 400));
    $httpClient->httpRequest($request3);

    # POSTed data was text
    my $request4 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://localhost:8080/post", 
                                                                                    arguments => ["TEXT", $testData], 
                                                                                    callback => $session->postback("httpClientPostback", undef, 400));
    $httpClient->httpRequest($request4);

    ### GET tests### 
    my $request5 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://localhost:8080/", 
                                                                                    callback => $session->postback("httpClientPostback", $testData, 200));
    $httpClient->httpRequest($request5);
    
    my $request6 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'GET', 
                                                                                    url => "http://localhost:8080/args",
                                                                                    arguments => {'test' => 'result'}, 
                                                                                    callback => $session->postback("httpClientPostback", $testData, 200));
    $httpClient->httpRequest($request6);

    # POST tests
    my $request7 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://localhost:8080/post", 
                                                                                    arguments => ["JSON", $testData], 
                                                                                    callback => $session->postback("httpClientPostback", undef, 200));
    $httpClient->httpRequest($request7);
    
    my $request8 = new PHEDEX::File::Download::Circuits::Helpers::HTTP::HttpRequest(method => 'POST', 
                                                                                    url => "http://localhost:8080/post", 
                                                                                    arguments => ["FORM", $testData], 
                                                                                    callback => $session->postback("httpClientPostback", undef, 200));
    $httpClient->httpRequest($request8);

    $kernel->delay("stopClient" => 2, $httpClient, $httpServer); # stop everything after 2 seconds
}

sub postbackForGetHandler {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($objectToProvide, $expectedArguments) = @{$initialArgs};
    my ($resultArguments, $resultCallback) = @{$postArgs};

    my $msg = "TestHTTPServer->postbackForGetHandler";
    is_deeply($resultArguments, $expectedArguments, "$msg: Resulted and expected arguments matched") if defined $expectedArguments;

    $resultCallback->($objectToProvide);
}

sub postbackForPostHandler {
     my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($expectedArguments) = @{$initialArgs};
    my ($resultArguments) = @{$postArgs};

    my $msg = "TestHTTPServer->postbackForPostHandler";
    is_deeply($resultArguments, $expectedArguments, "$msg: Resulted and expected arguments matched") if defined $expectedArguments;
}

sub httpClientPostback {
    my ($kernel, $session, $initialArgs, $postArgs) = @_[KERNEL, SESSION, ARG0, ARG1];

    my ($expectedObject, $expectedCode) = @{$initialArgs};
    my ($resultObject, $resultCode, $resultRequest) = @{$postArgs};

    my $uri = defined $resultRequest ? $resultRequest->{"_request"}->{"_uri"} : "unknown";

    my $msg = "TestHTTPServer->httpClientPostback";
    ok($postArgs, "$msg: ($uri) Arguments defined");

    is_deeply($resultObject, $expectedObject, "$msg: ($uri) Resulted and expected objects matched") if defined $expectedObject;
    is($resultCode, $expectedCode, "$msg: ($uri) Resulted and expected codes matched") if defined $expectedCode;

}

POE::Kernel->run();

done_testing;

1;