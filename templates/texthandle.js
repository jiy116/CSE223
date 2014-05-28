
(function() {
  var last = "";
  var lastCursor = 0;

  var $SCRIPT_ROOT = {{ request.script_root|tojson|safe }};

  //the logical clock we use
  var lClock = 0;

  //the class used to store the transferred data
  Data = function(changedString,oldCursor,newCursor,lClock) {
      this.changedString = changedString;
      this.oldCursor = oldCursor;
      this.newCursor = newCursor;
      this.lClock = lClock;
  }

  getCaret = function(node) {
    if (node.selectionStart) {
      return node.selectionStart;
    } else if (!document.selection) {
      return 0;
    }

    var c = "\001",
    sel = document.selection.createRange(),
    dul = sel.duplicate(),
    len = 0;

    dul.moveToElementText(node);
    sel.text = c;
    len = dul.text.indexOf(c);
    sel.moveStart('character',-1);
    sel.text = "";
    return len;
  }

  put = function(node){
    var textarea = document.getElementById('text1');
    var currCursor = getCaret(node);
    cur = node.value;
    var changedString = null;
    if(lastCursor < currCursor){
       changedString = cur.substring(lastCursor,currCursor);
    }
    else{
       ;
    }

    data = new Data(changedString,lastCursor,currCursor,lClock);
    lClock++;

    last = cur;
    lastCursor = currCursor;

    return changedString;
  }

  //to update the text
  updateText = function(data){
    //ret = JSON.parse(data);
    $("#text1").text(data.str + data.lClock);
  }

  $(document).ready(function(){
    $('textarea#text1').bind('input propertychange',function(){
        var getData = put(document.getElementById('text1'));
        alert(lastCursor);
        $.ajax({
          type: "GET",
          url: $SCRIPT_ROOT + "/echo/",
          data: JSON.stringify({
            changedString: getData,
            oldCursor: lastCursor,
            newCursor: currCursor,
            lClock: lClock
          }),
          success: updateText
        });
      });
  });
}).call(this);