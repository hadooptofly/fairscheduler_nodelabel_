# Nodelabel on fairscheduler
This repo is design for use nodeLabel based fairscheduler.

General functionality:
      1> App can specify nodeLabels it will request.
         contains:
                 container level specification.Such as you can let some container
                 run Label A, and some run on Label B, other run on Label C.
                 Like ML, HBase, Storm app will need this.
      2> Resource between Labels can get across and preempt mechanism will
         make resource guarantee(minshare,fairshare in FS) of a queue when 
         it need it again.
Like these scenario:
      1> ML, HBase, Storm.
      2> Someone buy some high equipment servers, and others can use when owner
         do not need, and return when he need again.
      
Like nodeLabel feature in CS, this feature will make fairscheduler schedule more 
scalability.
```xml
Up to now, 
     Queue config manual:
                which labels you can access:
                   <accessiableNodeLabel> ,kyb</accessiableNodeLabel>
                minshare of corresponding label as traditional：
                   <minResources label="">1024000 mb,1200 vcores,600 gcores</minResources>
                   <maxResources label="">3072000 mb,1600 vcores,600 gcores</maxResources>
                   <minResources label="kyb">1024000 mb,2000 vcores,600 gcores</minResources>
                   <maxResources label="kyb">3072000 mb,2000 vcores,900 gcores</maxResources>
                weight of corresponding label as traditional：
                   <weight label="">1.0</weight>
                   <weight label="kyb">1.0</weight>
     WebUi display:
                now support "Containers Running"	
                            "Memory Used"	"Memory Total"	
                            "Memory Reserved"	"VCores Used	
                            "VCores Total"	"VCores Reserved"	
                            "GCores Used"	"GCores Total"	
                            "GCores Reserved" 
             xml```           

                metrics label-differentiated:
                ![image](https://github.com/hadooptofly/pictures/blob/master/QQ20170827-195318%402x.png)
                queue detail:
                ![image](https://github.com/hadooptofly/pictures/blob/master/QQ20170827-195518%402x.png)
                resource allocation across labels default,kyb:
                ![image](https://github.com/hadooptofly/pictures/blob/master/QQ20170827-195538%402x.png)
