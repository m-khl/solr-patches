var loader = {
    
    show : function( element )
    {
        $( element )
            .addClass( 'loader' );
    },
    
    hide : function( element )
    {
        $( element )
            .removeClass( 'loader' );
    }
    
};

Number.prototype.esc = function()
{
    return new String( this ).esc();
}

String.prototype.esc = function()
{
    return this.replace( /</g, '&lt;' ).replace( />/g, '&gt;' );
}

var sammy = $.sammy
(
    function()
    {
        this.bind
        (
            'run',
            function( event, config )
            {
                if( 0 === config.start_url.length )
                {
                    location.href = '#/';
                    return false;
                }
            }
        );
        
        // activate_core
        this.before
        (
            {},
            function( context )
            {
                $( 'li[id].active', app.menu_element )
                    .removeClass( 'active' );
                
                $( 'ul li.active', app.menu_element )
                    .removeClass( 'active' );

                if( this.params.splat )
                {
                    var active_element = $( '#' + this.params.splat[0], app.menu_element );
                    
                    if( 0 === active_element.size() )
                    {
                        var first_core = $( 'li[data-basepath]', app.menu_element ).attr( 'id' );
                        var first_core_url = context.path.replace( new RegExp( '/' + this.params.splat[0] + '/' ), '/' + first_core + '/' );

                        context.redirect( first_core_url );
                        return false;
                    }

                    active_element
                        .addClass( 'active' );

                    if( this.params.splat[1] )
                    {
                        $( '.' + this.params.splat[1], active_element )
                            .addClass( 'active' );
                    }

                    if( !active_element.hasClass( 'global' ) )
                    {
                        this.active_core = active_element;
                    }
                }
            }
        );
    }
);

var solr_admin = function( app_config )
{
	self = this,

    menu_element = null,

    is_multicore = null,
    cores_data = null,
    active_core = null,
    environment_basepath = null,
    
    config = app_config,
    params = null,
    dashboard_values = null,
    schema_browser_data = null,

    plugin_data = null,
    
    this.menu_element = $( '#menu ul' );
    this.config = config;

    this.run = function()
    {
        $.ajax
        (
            {
                url : config.solr_path + config.core_admin_path + '?wt=json',
                dataType : 'json',
                beforeSend : function( arr, form, options )
                {               
                    $( '#content' )
                        .html( '<div id="index"><div class="loader">Loading ...</div></div>' );
                },
                success : function( response )
                {
                    self.cores_data = response.status;
                    is_multicore = 'undefined' === typeof response.status[''];

                    if( is_multicore )
                    {
                        self.menu_element
                            .addClass( 'multicore' );

                        $( '#cores', menu_element )
                            .show();
                    }
                    else
                    {
                        self.menu_element
                            .addClass( 'singlecore' );
                    }

                    for( var core_name in response.status )
                    {
                        var core_path = config.solr_path + '/' + core_name;

                        if( !core_name )
                        {
                            core_name = 'singlecore';
                            core_path = config.solr_path
                        }

                        if( !environment_basepath )
                        {
                            environment_basepath = core_path;
                        }

                        var core_tpl = '<li id="' + core_name + '" data-basepath="' + core_path + '">' + "\n"
                                     + '    <p><a href="#/' + core_name + '">' + core_name + '</a></p>' + "\n"
                                     + '    <ul>' + "\n"

                                     + '        <li class="ping"><a rel="' + core_path + '/admin/ping"><span>Ping</span></a></li>' + "\n"
                                     + '        <li class="query"><a href="#/' + core_name + '/query"><span>Query</span></a></li>' + "\n"
                                     + '        <li class="schema"><a href="#/' + core_name + '/schema"><span>Schema</span></a></li>' + "\n"
                                     + '        <li class="config"><a href="#/' + core_name + '/config"><span>Config</span></a></li>' + "\n"
                                     + '        <li class="replication"><a href="#/' + core_name + '/replication"><span>Replication</span></a></li>' + "\n"
                                     + '        <li class="analysis"><a href="#/' + core_name + '/analysis"><span>Analysis</span></a></li>' + "\n"
                                     + '        <li class="schema-browser"><a href="#/' + core_name + '/schema-browser"><span>Schema Browser</span></a></li>' + "\n"
                                     + '        <li class="plugins"><a href="#/' + core_name + '/plugins"><span>Plugins</span></a></li>' + "\n"
                                     + '        <li class="dataimport"><a href="#/' + core_name + '/dataimport"><span>Dataimport</span></a></li>' + "\n"

                                     + '    </ul>' + "\n"
                                     + '</li>';

                        self.menu_element
                            .append( core_tpl );
                    }

                    $.ajax
                    (
                        {
                            url : environment_basepath + '/admin/system?wt=json',
                            dataType : 'json',
                            beforeSend : function( arr, form, options )
                            {
                            },
                            success : function( response )
                            {
                                self.dashboard_values = response;

                                var environment_args = null;
                                var cloud_args = null;

                                if( response.jvm && response.jvm.jmx && response.jvm.jmx.commandLineArgs )
                                {
                                    var command_line_args = response.jvm.jmx.commandLineArgs.join( ' | ' );

                                    environment_args = command_line_args
                                                            .match( /-Dsolr.environment=((dev|test|prod)?[\w\d]*)/i );

                                    cloud_args = command_line_args
                                                            .match( /-Dzk/i );
                                }

                                // environment

                                var environment_element = $( '#environment' );
                                if( environment_args )
                                {
                                    environment_element
                                        .show();

                                    if( environment_args[1] )
                                    {
                                        environment_element
                                            .html( environment_args[1] );
                                    }

                                    if( environment_args[2] )
                                    {
                                        environment_element
                                            .addClass( environment_args[2] );
                                    }
                                }
                                else
                                {
                                    environment_element
                                        .remove();
                                }

                                // cloud

                                var cloud_nav_element = $( '#menu #cloud' );
                                if( cloud_args )
                                {
                                    cloud_nav_element
                                        .show();
                                }

                                // sammy

                                sammy.run( location.hash );
                            },
                            error : function()
                            {
                            },
                            complete : function()
                            {
                                loader.hide( this );
                            }
                        }
                    );
                },
                error : function()
                {
                },
                complete : function()
                {
                }
            }
        );
    }

};

var app = new solr_admin( app_config );