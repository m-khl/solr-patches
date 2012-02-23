sammy.bind
(
    'dataimport_queryhandler_load',
    function( event, params )
    {
        var core_basepath = params.active_core.attr( 'data-basepath' );

        $.ajax
        (
            {
                url : core_basepath + '/admin/mbeans?cat=QUERYHANDLER&wt=json',
                dataType : 'json',
                beforeSend : function( xhr, settings )
                {
                },
                success : function( response, text_status, xhr )
                {
                    var handlers = response['solr-mbeans'][1];
                    var dataimport_handlers = [];
                    for( var key in handlers )
                    {
                        if( handlers[key]['class'] !== key &&
                            handlers[key]['class'] === 'org.apache.solr.handler.dataimport.DataImportHandler' )
                        {
                            dataimport_handlers.push( key );
                        }
                    }
                    params.callback( dataimport_handlers );
                },
                error : function( xhr, text_status, error_thrown)
                {
                },
                complete : function( xhr, text_status )
                {
                }
            }
        );
    }
);

// #/:core/dataimport
sammy.get
(
    /^#\/([\w\d-]+)\/(dataimport)$/,
    function( context )
    {
        sammy.trigger
        (
            'dataimport_queryhandler_load',
            {
                active_core : this.active_core,
                callback :  function( dataimport_handlers )
                {
                    if( 0 === dataimport_handlers.length )
                    {
                        $( '#content' )
                            .html( 'sorry, no dataimport-handler defined!' );

                        return false;
                    }

                    context.redirect( context.path + '/' + dataimport_handlers[0] );
                }
            }
        );
    }
);

// #/:core/dataimport
sammy.get
(
    /^#\/([\w\d-]+)\/(dataimport)\//,
    function( context )
    {
        var core_basepath = this.active_core.attr( 'data-basepath' );
        var content_element = $( '#content' );

        var path_parts = this.path.match( /^(.+\/dataimport\/)(.*)$/ );
        var handler_url = core_basepath + path_parts[2];
        
        $( 'li.dataimport', this.active_core )
            .addClass( 'active' );

        $.get
        (
            'tpl/dataimport.html',
            function( template )
            {
                content_element
                    .html( template );

                var dataimport_element = $( '#dataimport', content_element );
                var form_element = $( '#form', dataimport_element );
                var config_element = $( '#config', dataimport_element );
                var config_error_element = $( '#config-error', dataimport_element );

                // handler

                sammy.trigger
                (
                    'dataimport_queryhandler_load',
                    {
                        active_core : context.active_core,
                        callback :  function( dataimport_handlers )
                        {

                            var handlers_element = $( '.handler', form_element );
                            var handlers = [];

                            for( var i = 0; i < dataimport_handlers.length; i++ )
                            {
                                handlers.push
                                (
                                        '<li><a href="' + path_parts[1] + dataimport_handlers[i] + '">' +
                                        dataimport_handlers[i] +
                                        '</a></li>'
                                );
                            }

                            $( 'ul', handlers_element )
                                .html( handlers.join( "\n") ) ;
                            
                            $( 'a[href="' + context.path + '"]', handlers_element ).parent()
                                .addClass( 'active' );
                            
                            handlers_element
                                .show();
                        }
                    }
                );

                // config

                function dataimport_fetch_config()
                {
                    $.ajax
                    (
                        {
                            url : handler_url + '?command=show-config',
                            dataType : 'xml',
                            context : $( '#dataimport_config', config_element ),
                            beforeSend : function( xhr, settings )
                            {
                            },
                            success : function( config, text_status, xhr )
                            {
                                dataimport_element
                                    .removeClass( 'error' );
                                    
                                config_error_element
                                    .hide();

                                config_element
                                    .addClass( 'hidden' );


                                var entities = [];

                                $( 'document > entity', config )
                                    .each
                                    (
                                        function( i, element )
                                        {
                                            entities.push( '<option>' + $( element ).attr( 'name' ) + '</option>' );
                                        }
                                    );
                                
                                $( '#entity', form_element )
                                    .append( entities.join( "\n" ) );
                            },
                            error : function( xhr, text_status, error_thrown )
                            {
                                if( 'parsererror' === error_thrown )
                                {
                                    dataimport_element
                                        .addClass( 'error' );
                                    
                                    config_error_element
                                        .show();

                                    config_element
                                        .removeClass( 'hidden' );
                                }
                            },
                            complete : function( xhr, text_status )
                            {
                                var code = $(
                                    '<pre class="syntax language-xml"><code>' +
                                    xhr.responseText.replace( /\</g, '&lt;' ).replace( /\>/g, '&gt;' ) +
                                    '</code></pre>'
                                );
                                this.html( code );

                                if( 'success' === text_status )
                                {
                                    hljs.highlightBlock( code.get(0) );
                                }
                            }
                        }
                    );
                }
                dataimport_fetch_config();

                $( '.toggle', config_element )
                    .die( 'click' )
                    .live
                    (
                        'click',
                        function( event )
                        {
                            $( this ).parents( '.block' )
                                .toggleClass( 'hidden' );
                            
                            return false;
                        }
                    )

                var reload_config_element = $( '.reload_config', config_element );
                reload_config_element
                    .die( 'click' )
                    .live
                    (
                        'click',
                        function( event )
                        {
                            $.ajax
                            (
                                {
                                    url : handler_url + '?command=reload-config',
                                    dataType : 'xml',
                                    context: $( this ),
                                    beforeSend : function( xhr, settings )
                                    {
                                        this
                                            .addClass( 'loader' );
                                    },
                                    success : function( response, text_status, xhr )
                                    {
                                        this
                                            .addClass( 'success' );

                                        window.setTimeout
                                        (
                                            function()
                                            {
                                                reload_config_element
                                                    .removeClass( 'success' );
                                            },
                                            5000
                                        );
                                    },
                                    error : function( xhr, text_status, error_thrown )
                                    {
                                        this
                                            .addClass( 'error' );
                                    },
                                    complete : function( xhr, text_status )
                                    {
                                        this
                                            .removeClass( 'loader' );
                                        
                                        dataimport_fetch_config();
                                    }
                                }
                            );
                            return false;
                        }
                    )

                // state
                
                function dataimport_fetch_status()
                {
                    $.ajax
                    (
                        {
                            url : handler_url + '?command=status',
                            dataType : 'xml',
                            beforeSend : function( xhr, settings )
                            {
                            },
                            success : function( response, text_status, xhr )
                            {
                                var state_element = $( '#current_state', content_element );

                                var status = $( 'str[name="status"]', response ).text();
                                var rollback_element = $( 'str[name="Rolledback"]', response );
                                var messages_count = $( 'lst[name="statusMessages"] str', response ).size();

                                var started_at = $( 'str[name="Full Dump Started"]', response ).text();
                                if( !started_at )
                                {
                                    started_at = (new Date()).toGMTString();
                                }

                                function dataimport_compute_details( response, details_element )
                                {
                                    var details = [];
                                    
                                    var requests = parseInt( $( 'str[name="Total Requests made to DataSource"]', response ).text() );
                                    if( NaN !== requests )
                                    {
                                        details.push
                                        (
                                            '<abbr title="Total Requests made to DataSource">Requests</abbr>: ' +
                                            requests
                                        );
                                    }

                                    var fetched = parseInt( $( 'str[name="Total Rows Fetched"]', response ).text() );
                                    if( NaN !== fetched )
                                    {
                                        details.push
                                        (
                                            '<abbr title="Total Rows Fetched">Fetched</abbr>: ' +
                                            fetched
                                        );
                                    }

                                    var skipped = parseInt( $( 'str[name="Total Documents Skipped"]', response ).text() );
                                    if( NaN !== requests )
                                    {
                                        details.push
                                        (
                                            '<abbr title="Total Documents Skipped">Skipped</abbr>: ' +
                                            skipped
                                        );
                                    }

                                    var processed = parseInt( $( 'str[name="Total Documents Processed"]', response ).text() );
                                    if( NaN !== processed )
                                    {
                                        details.push
                                        (
                                            '<abbr title="Total Documents Processed">Processed</abbr>: ' +
                                            processed
                                        );
                                    }

                                    details_element
                                        .html( details.join( ', ' ) );
                                }

                                state_element
                                    .removeClass( 'indexing' )
                                    .removeClass( 'success' )
                                    .removeClass( 'failure' );
                                
                                $( '.info', state_element )
                                    .removeClass( 'loader' );

                                if( 0 !== rollback_element.size() )
                                {
                                    state_element
                                        .addClass( 'failure' )
                                        .show();

                                    $( '.info strong', state_element )
                                        .text( $( 'str[name=""]', response ).text() );
                                    
                                    console.debug( 'rollback @ ', rollback_element.text() );
                                }
                                else if( 'idle' === status && 0 !== messages_count )
                                {
                                    state_element
                                        .addClass( 'success' )
                                        .show();

                                    $( '.time', state_element )
                                        .text( started_at )
                                        .timeago();

                                    $( '.info strong', state_element )
                                        .text( $( 'str[name=""]', response ).text() );

                                    dataimport_compute_details( response, $( '.info .details', state_element ) );
                                }
                                else if( 'busy' === status )
                                {
                                    state_element
                                        .addClass( 'indexing' )
                                        .show();

                                    $( '.time', state_element )
                                        .text( started_at )
                                        .timeago();

                                    $( '.info', state_element )
                                        .addClass( 'loader' );

                                    $( '.info strong', state_element )
                                        .text( 'Indexing ...' );
                                    
                                    dataimport_compute_details( response, $( '.info .details', state_element ) );

                                    window.setTimeout( dataimport_fetch_status, 2000 );
                                }
                                else
                                {
                                    state_element.hide();
                                }
                            },
                            error : function( xhr, text_status, error_thrown )
                            {
                                console.debug( arguments );
                            },
                            complete : function( xhr, text_status )
                            {
                            }
                        }
                    );
                }
                dataimport_fetch_status();

                // form

                $( 'form', form_element )
                    .die( 'submit' )
                    .live
                    (
                        'submit',
                        function( event )
                        {
                            $.ajax
                            (
                                {
                                    url : handler_url + '?command=full-import',
                                    dataType : 'xml',
                                    beforeSend : function( xhr, settings )
                                    {
                                    },
                                    success : function( response, text_status, xhr )
                                    {
                                        console.debug( response );
                                        dataimport_fetch_status();
                                    },
                                    error : function( xhr, text_status, error_thrown )
                                    {
                                        console.debug( arguments );
                                    },
                                    complete : function( xhr, text_status )
                                    {
                                    }
                                }
                            );
                            return false;
                        }
                    );
            }
        );
    }
);