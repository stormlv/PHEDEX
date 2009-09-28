// instantiate the PHEDEX.Widget.Nodes namespace
PHEDEX.namespace('Widget.Nodes');

PHEDEX.Page.Widget.Nodes=function(divid) {
  var node = document.getElementById(divid+'_select').value;
//   var nodes = new PHEDEX.Widget.Nodes(node,divid);
//   nodes.update();
  var nodes = PHEDEX.Core.Widget.Registry.construct('PHEDEX.Widget.Nodes','node',node,divid);
  nodes.update();
}

// Note:  'node' is ignored
PHEDEX.Widget.Nodes=function(node,divid,opts) {
  if ( !divid) { divid = PHEDEX.Util.generateDivName(); }
  if ( !opts)  { opts = {} };
  // Merge passed options with defaults
  YAHOO.lang.augmentObject(opts, { 
    width:500,
    height:200,
    minwidth:300,
    minheight:50,
    defsort:'Name',
    defhide:['Kind','Technology']
  });
 
  var that=new PHEDEX.Core.Widget.DataTable(divid+'_display', opts);
  that.node=node;
  that.data = null;
  that._me='PHEDEX.Core.Widget.Nodes';
  that.me=function() { return that._me; }
  that.filter.init( {
    'Node attributes':{
      'id'         :{type:'int',   text:'Node-ID',    tip:'Node-ID in TMDB' },
      'name'       :{type:'regex', text:'Node-name',  tip:'javascript regular expression' },
      'se'         :{type:'regex', text:'SE-name',    tip:'javascript regular expression' },
      'kind'       :{type:'regex', text:'Kind',       tip:'javascript regular expression' },
      'technology' :{type:'regex', text:'Technology', tip:'javascript regular expression' }
    } } );

  that.fillHeader=function(div) {
    that.dom.title.innerHTML = 'Nodes: '+this.data.length+" found";
  }
  that.buildTable(that.dom.content,
		  [ {key:'ID',parser:YAHOO.util.DataSource.parseNumber },'Name','Kind','Technology','SE' ]
		 );
  that.update=function() {
    PHEDEX.Datasvc.Call({ api: 'nodes', success_event: that.onDataReady });
  }
  that.onDataReady.subscribe(function(type,args) { var data = args[0]; that.receive(data); });
  that.receive=function(data) {
    that.data = data.node;
    if (that.data) { that.populate(); }
    else { that.failedLoading(); }
  }
  that.buildExtra(that.dom.extra);
  that.buildContextMenu({'node':'Name'});
  that.build();
  that.ctl.extra.Disable();
  return that;
}

PHEDEX.Core.Widget.Registry.add('PHEDEX.Widget.Nodes','none','Show Nodes',PHEDEX.Widget.Nodes);
