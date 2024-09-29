# Authenticated Range querying of Blockchain Data using Authenticated AVL Tree
* University of New Brunswick
* Big Data Systems Course - CS6545
* Instructor: Dr. Suprio Ray
* Winter 2023

Contributors
* @ph504, arya.parvizi@unb.ca 
* @arbaaz-abz, arbaaz.dm@unb.ca

## Abstract
In the rapidly evolving domain of blockchain data management, the need for authenticated data structures that can facilitate both integrity verification and efficient data querying has become increasingly necessary. This project proposes an Authenticated AVL Tree as an alternative to the Authenticated Multi-Version Skip List (AMVSL), capitalizing on its deterministic nature, intrinsic self-balancing properties, and the simplicity of its operational algorithms. Contrary to the AMVSL’s probabilistic approach, which may yield variance in individual query execution times, the AVL Tree ensures predictable logarithmic performance across all operations. Our analysis and implementation reveal that the AVL Tree not only simplifies complexity but also marginally enhances range querying speed, outperforming AMVSL by 1.2 to 3.7 times in three range queries: SVRK, MVRK, and MVAK across various configurational parameters, despite a marginal slowdown of 0.83x during the insertion phase, however we later show that this slowdown occurs only during a specific dataset configuration. This performance benefit is not only observed with a small number of keys, where execution time predictability is paramount, but also scales significantly with larger datasets, where the gains in query performance become even more pronounced. This report demonstrates the AVL Tree’s potential to streamline and simplify authenticated historical querying in blockchain databases, while promising a robust and reliable framework for data analysis tasks. 
