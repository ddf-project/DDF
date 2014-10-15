$(document).ready(function(){ 

    $(document).foundation({
      tab: {
        callback : function (tab) {
          console.log(tab);
        }
      }
    });

    console.log(window.location.hash)

    if(window.location.hash != ''){
    	if($('iframe[name=content]').size() > 0){
    		$('iframe[name=content]').attr('src', window.location.hash.substring(1))
    	}
    }

    $('.iframe-links a').click(function(){
    	var link = $(this).attr('href')
    	window.location.hash = link;
    })

});