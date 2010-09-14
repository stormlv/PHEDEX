PHEDEX.namespace('Module');

PHEDEX.Module.DataBrowser=function(sandbox, string) {
  var _sbx = sandbox,
      dataset, block;
      opts = {
        file_create_since:    9999,
        block_create_since:   24,
        dataset_create_since: 9999,
        file: null,
        block: null,
        dataset: null
      },
      width = 1200;

  Yla(this,new PHEDEX.TreeView(sandbox,string));
  Yla(opts, {
    width:width,
    height:300
  });

  log('Module: creating a genuine "'+string+'"','info',string);

  _construct = function() {
    return {
      decorators: [
        {
          name: 'Headers',
          source:'component-control',
          parent: 'control',
          payload:{
            target: 'extra',
            animate:false
          }
        },
        {
          name: 'Subscribe',
          text: 'Make a subscription',
          source:'component-subscribe',
          payload:{
            panel: {
              'Datasets':{
                map:{from:'phedex-panel-dataset-', to:'P-D'},
                fields:{
                  'dataset' :{type:'input', tip:'Dataset name, with or without wildcards', dynamic:true },
                }
              },
              'Blocks':{
                map:{from:'phedex-panel-block-', to:'P-B'},
                fields:{
                  'block' :{type:'input', tip:'Block name, with or without wildcards', dynamic:true },
                }
              }
            },
            control: {
              parent: 'control',
              payload:{
                text:'Subscribe',
                animate:  false,
                disabled: true,
//                 hidden:   true
              },
              el: 'content'
            }
          }
        },
        {
          name: 'ContextMenu',
          source:'component-contextmenu'
        },
        {
          name: 'cMenuButton',
          source:'component-splitbutton',
          payload:{
            name:'Show all fields',
            map: {
              hideColumn:'addMenuItem'
            },
            container: 'buttons'
          }
        },
//         {
//           name: 'FileCreateTimeSelect',
//           source: 'component-menu',
//           payload:{
//             type: 'menu',
//             initial: function() { return opts.file_create_since; },
//             container: 'buttons',
//             prefix:'File Created:',
//             menu: { 1:'Last Hour', 3:'Last 3 Hours', 6:'Last 6 Hours', 12:'Last 12 Hours', 24:'Last Day', 48:'Last 2 Days', 96:'Last 4 Days', 168:'Last Week', 336:'Last 2 weeks', 672:'Last 4 Weeks', 1342:'Last 8 Weeks', 9999:'Forever' },
//             map: {
//               onChange:'changeFileCreateTimebin',
//             },
//             title:'Time since File creation'
//           }
//         },
        {
          name: 'BlockCreateTimeSelect',
          source: 'component-menu',
          payload:{
            type: 'menu',
            initial: function() { return opts.block_create_since; },
            container: 'buttons',
            prefix:'Block Created:',
            menu: { 1:'Last Hour', 3:'Last 3 Hours', 6:'Last 6 Hours', 12:'Last 12 Hours', 24:'Last Day', 48:'Last 2 Days', 96:'Last 4 Days', 168:'Last Week', 336:'Last 2 weeks', 672:'Last 4 Weeks', 1342:'Last 8 Weeks', 9999:'Forever' },
            map: {
              onChange:'changeBlockCreateTimebin'
            },
            title:'Time since Block creation'
          }
        },
        {
          name: 'DatasetCreateTimeSelect',
          source: 'component-menu',
          payload:{
            type: 'menu',
            initial: function() { return opts.dataset_create_since; },
            container: 'buttons',
            prefix:'Dataset Created:',
            menu: { 1:'Last Hour', 3:'Last 3 Hours', 6:'Last 6 Hours', 12:'Last 12 Hours', 24:'Last Day', 48:'Last 2 Days', 96:'Last 4 Days', 168:'Last Week', 336:'Last 2 weeks', 672:'Last 4 Weeks', 1342:'Last 8 Weeks', 9999:'Forever' },
            map: {
              onChange:'changeDatasetCreateTimebin'
            },
            title:'Time since Dataset creation'
          }
        }
      ],

      meta: {
        tree: [
          {
            width:1200,
            name:'Dataset',
            format: [
              {width:600,text:'Dataset Name', className:'phedex-tree-dataset-name',       otherClasses:'align-left',  ctxArgs:['dataset','sort-alpha'], ctxKey:'dataset', spanWrap:true },
              {width: 60,text:'Open',         className:'phedex-tree-dataset-open',       otherClasses:'align-right', ctxArgs:'sort-alpha' },
              {width: 60,text:'Transient',    className:'phedex-tree-dataset-transient',  otherClasses:'align-right', ctxArgs:'sort-alpha' },
              {width:160,text:'Create Time',  className:'phedex-tree-dataset-timecreate', otherClasses:'align-right', ctxArgs:'sort-alpha', format:'UnixEpochToGMT' },
              {width:160,text:'Update Time',  className:'phedex-tree-dataset-timeupdate', otherClasses:'align-right', ctxArgs:'sort-alpha', format:'UnixEpochToGMT', hide:true }
            ]
          },
          {
            width:1200,
            name:'Block',
            format: [
              {width:600,text:'Block Name',  className:'phedex-tree-block-name',       otherClasses:'align-left',  ctxArgs:['block','sort-alpha'], ctxKey:'block', spanWrap:true },
              {width: 60,text:'Files',       className:'phedex-tree-block-files',      otherClasses:'align-right', ctxArgs:'sort-num' },
              {width: 80,text:'Bytes',       className:'phedex-tree-block-bytes',      otherClasses:'align-right', ctxArgs:'sort-num', format:PxUf.bytes },
              {width: 60,text:'Open',        className:'phedex-tree-block-open',       otherClasses:'align-right', ctxArgs:'sort-alpha' },
              {width:160,text:'Create Time', className:'phedex-tree-block-timecreate', otherClasses:'align-right', ctxArgs:'sort-alpha', format:'UnixEpochToGMT' },
              {width:160,text:'Update Time', className:'phedex-tree-block-timeupdate', otherClasses:'align-right', ctxArgs:'sort-alpha', format:'UnixEpochToGMT', hide:true }
            ]
          },
          {
            name:'Files',
            format:[
              {width:600,text:'File Name',   className:'phedex-tree-file-name',       otherClasses:'align-left',  ctxArgs:'sort-alpha', spanWrap:true },
              {width:160,text:'Node',        className:'phedex-tree-file-node',       otherClasses:'align-left',  ctxArgs:['node','sort-alpha'], ctxKey:'node', hide:true },
              {width: 80,text:'Bytes',       className:'phedex-tree-file-bytes',      otherClasses:'align-right', ctxArgs:'sort-num', format:PxUf.bytes },
              {width:160,text:'Create Time', className:'phedex-tree-file-timecreate', otherClasses:'align-right', ctxArgs:'sort-alpha', format:'UnixEpochToGMT' },
              {width:140,text:'Checksum',    className:'phedex-tree-file-cksum',      otherClasses:'align-right', hide:true }
            ]
          }
        ],
// Filter-structure mimics the branch-structure. Use the same classnames as keys.
        filter: {
          'Dataset-level attributes':{
            map:{from:'phedex-tree-dataset-', to:'D'},
            fields:{
              'phedex-tree-dataset-name'       :{type:'regex',  text:'Block-name',  tip:'javascript regular expression' },
              'phedex-tree-dataset-open'       :{type:'yesno',  text:'Open',        tip:'is the block still open?' },
              'phedex-tree-dataset-transient'  :{type:'yesno',  text:'Transient',   tip:'is the block transient?' },
              'phedex-tree-dataset-timecreate' :{type:'minmax', text:'Create Time', tip:'unix epoch seconds' },
              'phedex-tree-dataset-timeupdate' :{type:'minmax', text:'Update Time', tip:'unix epoch seconds' }
            }
          },
          'Block-level attributes':{
            map:{from:'phedex-tree-block-', to:'B'},
            fields:{
              'phedex-tree-block-name'       :{type:'regex',  text:'Block-name',  tip:'javascript regular expression' },
              'phedex-tree-block-files'      :{type:'minmax', text:'Block-files', tip:'number of files in the block' },
              'phedex-tree-block-bytes'      :{type:'minmax', text:'Block-bytes', tip:'number of bytes in the block' },
              'phedex-tree-block-open'       :{type:'yesno',  text:'Open',        tip:'is the block still open?' },
              'phedex-tree-block-timecreate' :{type:'minmax', text:'Create Time', tip:'unix epoch seconds' },
              'phedex-tree-block-timeupdate' :{type:'minmax', text:'Update Time', tip:'unix epoch seconds' }
            }
          },
          'File-level attributes':{
            map:{from:'phedex-tree-file-', to:'F'},
            fields:{
              'phedex-tree-file-name'       :{type:'regex',  text:'File-name',   tip:'javascript regular expression' },
              'phedex-tree-file-node'       :{type:'regex',  text:'File-node',   tip:'javascript regular expression' },
              'phedex-tree-file-bytes'      :{type:'minmax', text:'File-bytes',  tip:'number of bytes in the file' },
              'phedex-tree-file-timecreate' :{type:'minmax', text:'Create Time', tip:'unix epoch time' },
              'phedex-tree-file-checksum'   :{type:'regex',  text:'Checksum',    tip:'javascript regular expression' }
            }
          }
        }
      },

      initMe: function(){ },

      api_keys: {dataset_create_since:1 ,block_create_since:1/*, file_create_since:1*/},
      specificState: function(state) {
        var s, i, j, k, v, kv, update, arr;
        if ( !state ) {
          s = {};
          for (i in this.api_keys) {
            if ( opts[i] ) { s[i] = opts[i]; }
          }
          return s;
        }
        update=0;
        arr = state.split(' ');
        for (i in arr) {
          kv = arr[i].split('=');
          k = kv[0];
          v = kv[1];
          if ( this.api_keys[k] && v != opts[k] ) { update++; opts[k] = v; }
        }
        if ( !update ) { return; }
        this.getData();
      },

      changeDatasetCreateTimebin: function(arg) {
        opts.dataset_create_since = parseInt(arg);
        this.getData();
      },
      changeBlockCreateTimebin: function(arg) {
        opts.block_create_since = parseInt(arg);
        this.getData();
      },
      changeFileCreateTimebin: function(arg) {
        opts.file_create_since = parseInt(arg);
        this.getData();
      },
      fillBody: function() {
        var root = this.tree.getRoot(),
            tLeaf, tNode, tNode1, tNode2, i, j, k, datasets=[], d, blocks, b, bf, files, f, totFiles,
            dbs = this.data.dbs;
        if ( !dbs.length )
        {
          tLeaf = new Yw.TextNode({label: 'Nothing found, try another dataset or block...', expanded: false}, root);
          tLeaf.isLeaf = true;
        } else {
          datasets = dbs[0].dataset;
        }
        for (i in datasets) {
          d = datasets[i];
          tNode = this.addNode(
            { format:this.meta.tree[0].format },
            [ d.name,d.is_open,d.is_transient,d.time_update,d.time_create ]
          );
          if ( d.block ) {
            totFiles=0;
            if ( d.block.length == 1 ) { tNode.title = '1 block'; }
            else                       { tNode.title = d.block.length+' blocks'; }
            for (j in d.block) {
              b = d.block[j];
              tNode1 = this.addNode(
                { format:this.meta.tree[1].format },
                [ b.name,b.files,b.bytes,b.is_open,b.time_create,b.time_update ],
                tNode
              );
              if ( b.file ) {
                bf = b.file.length;
                totFiles += bf;
                if ( bf == 1 ) { tNode1.title = '1 file'; }
                else           { tNode1.title = bf+' files'; }
                for (k in b.file) {
                  f = b.file[k];
                  tNode2 = this.addNode(
                    { format:this.meta.tree[2].format },
                    [ f.lfn,f.node,f.size,f.time_create,f.checksum ],
                    tNode1
                  );
                }
              } else { tNode1.isLeaf = true; }
            }
            tNode.title += ', '+totFiles+' files';
          } else { tNode.isLeaf = true; }
        }
        this.tree.render();
      },

      initData: function() {
        this.dom.title.innerHTML = 'Waiting for parameters to be set...';
        if ( dataset || block ) {
          _sbx.notify( this.id, 'initData' );
          return;
        }
        _sbx.notify( 'module', 'needArguments', this.id );
      },
/** Call this to set the parameters of this module and cause it to fetch new data from the data-service.
 * @method setArgs
 * @param arr {array} object containing arguments for this module. Highly module-specific! For the <strong>Agents</strong> module, only <strong>arr.node</strong> is required. <strong>arr</strong> may be null, in which case no data will be fetched.
 */
      setArgs: function(arr) {
        if ( arr && typeof(arr) == 'object' ) {
         dataset = arr.dataset || dataset;
         block   = arr.block   || block;
          if ( !dataset && !block ) { return; }
          if (  dataset && block ) { block = null; }
          this.dom.title.innerHTML = 'setting parameters...';
          _sbx.notify(this.id,'setArgs');
        }
      },
      getData: function() {
        if ( !dataset && !block ) {
          this.initData();
          return;
        }
        log('Fetching data','info',this.me);
        this.dom.title.innerHTML = 'fetching data...';
        var args = {}, magic = PxU.Sequence(), // TODO need better magic than this!
          now;
        if ( this._magic == magic ) {
          log('Already asked for this magic data: magic="'+magic+'"','warn',this.me);
          return;
        }
        this._magic = magic;
        if ( block ) { args.block = block; node = null; }
        if ( node  ) { args.node  = node; }
        now = PxU.epochAlign();

        for (i in this.api_keys) {
          if ( opts[i] ) {
            if ( opts[i] == 9999 ) {
              args[i] = 0;
            } else {
              args[i] = now - 3600 * opts[i];
            }
          }
        }
        this.data = {};
        this.truncateTree();
        this.tree.render();
        _sbx.notify( this.id, 'getData', { api:'data', args:args, magic:magic } );
      },
      gotData: function(data,context) {
        log('Got new data: api='+context.api+', id='+context.poll_id+', magic:'+context.magic,'info',this.me);
        if ( this._magic != context.magic ) {
          log('Old data has lost its magic: "'+this._magic+'" != "'+context.magic+'"','warn',this.me);
          return;
        }
        if ( !data.dbs ) {
          throw new Error('data incomplete for '+context.api);
        }
        this.data = data;
        this._magic = null;
        this.dom.title.innerHTML = ( dataset ? 'dataset='+dataset : '' ) + ( dataset && block ? ', ' : '' ) + ( block ? 'block='+block : '' );
        this.fillBody();
        _sbx.notify( this.id, 'gotData' );
      }
    };
  };
  Yla(this,_construct(),true);
  return this;
}

log('loaded...','info','databrowser');