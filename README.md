# About:
This repository contains the code for my bachelor thesis about Multi-Attribute Join Search with MapReduce that was written at Leibniz University Hanover in 2021.

## Abstract:
To take full advantage of growing data lakes, search algorithms need to be more efficient than ever. Many recent works in the field of data discovery focus on single attribute join search, or similarity search using metadata. However, these approaches often lack accuracy, when looking for data to enrich machine learning datasets. In such cases, a multi-attribute join search can deliver closely related and diverse data. Also, the discovery of previously unknown causalities is possible with a multi-attribute join.  

This thesis presents two multi-attribute join search algorithms: A sequential one, named Seq-Search and one using MapReduce and parallelization, named Par-Search. The research focuses on the runtime improvement, which can be achieved using parallelization and MapReduce in the context of join search. Additionally, on an inverted-index setup, the question of the ideal query strategy and the join search discovery level is answered.  

The conducted experiments show, that Par-Search can achieve a ten-fold speedup when running on a 128 core system. It is also demonstrated in experiments with Seq-Search, that a column-wise query strategy is the most effective approach in this setup. Also, join discovery on the python-level is much more efficient than join discovery on the SQL-level in this case.

## Prerequisites:
- requirements.txt
- db credentials stored in environmental variables  
  (VPORT, VHOST, VUSER, VPASSW and VSESSIONLABEL)
