# fairscheduler_nodelabel_
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

Thanks Wangda Tan for the nodeLabel works in Yarn.
