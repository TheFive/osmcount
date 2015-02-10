function cleanObject(obj) {
	
     console.log("Enterong clean Object");
     for (k in obj) {
     	console.log(k);
     	if (typeof (obj[k]) =='object') {
     	  cleanObject(obj[k]);
     	}
        console.log(k.indexOf('.'));
        
     	if (k.indexOf(".") >0 ) {
     	  console.log( "delete "+k);
     	    //console.log("Fount ." +k.search("."));
     		//console.dir(data);
     		
     		delete obj[k]; 
     		//console.log("After Delete");
     		//console.dir(data);
     	}
     }
   }
   
   
   
o = { tag :1, tag2:2, tag3:2, subtags: {"asdf.asdf":4,asdf:3}}

cleanObject(o);

console.dir(o);
