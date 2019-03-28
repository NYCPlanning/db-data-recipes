for TT in $(mdb-tables /recipes/dcp_pops/tmp/DCP_POPS_NYC_BigApps.mdb); do
     mdb-export -D '%Y-%m-%d %H:%M:%S' /recipes/dcp_pops/tmp/DCP_POPS_NYC_BigApps.mdb "$TT" > "/recipes/dcp_pops/tmp/${TT}.csv"
done