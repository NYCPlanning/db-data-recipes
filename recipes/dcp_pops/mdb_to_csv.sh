for TT in $(mdb-tables tmp/DCP_POPS_NYC_BigApps.mdb); do
     mdb-export -D '%Y-%m-%d %H:%M:%S' tmp/DCP_POPS_NYC_BigApps.mdb "$TT" > "tmp/${TT}.csv"
done