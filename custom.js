$(document).ready(function(){

  $('.submitbtn').click(function(){
    var hm = "heatmap=" + $('#heatmap').is(':checked');
    var sg = "graph=" + $('#graph').is(':checked');
    var uh = "uh=" + $('#highlighter').is(':checked');
    var config = hm + "<br>" + sg + "<br>" + uh;

    document.getElementById("output").innerHTML = "<b>Configurating ElasticSearch features as:</b><br>" + config;


    var textToSave = hm + "\r\n" + sg + "\r\n" + uh;
    var textToSaveAsBlob = new Blob([textToSave], {type:"text"});
    var textToSaveAsURL = window.URL.createObjectURL(textToSaveAsBlob);

    var downloadLink = document.createElement("a");
    downloadLink.download = "config.properties";
    downloadLink.innerHTML = "Download File";
    downloadLink.href = textToSaveAsURL;
    downloadLink.onclick = destroyClickedElement;
    downloadLink.style.display = "none";
    document.body.appendChild(downloadLink);

    downloadLink.click();

  });

  function destroyClickedElement(event)
  {
      document.body.removeChild(event.target);
  }




});
