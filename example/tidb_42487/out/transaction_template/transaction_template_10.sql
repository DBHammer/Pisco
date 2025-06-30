start transaction;
select `pkAttr0`, `pkAttr1`, `pkAttr2`, `pkAttr3`, `coAttr0_0`, `coAttr0_1` from `table0` where ( `pkAttr0`  =  ?  ) or ( `pkAttr1`  =  ?  ) or ( `pkAttr2`  =  ?  ) or ( `pkAttr3`  =  ?  ) order by `pkAttr0`, `pkAttr1`, `pkAttr2` ;
update `table0` set `coAttr0_0` = ?, `coAttr0_1` = ? where ( `pkAttr0`  =  ?  ) and ( `pkAttr1`  =  ?  ) and ( `pkAttr2`  =  ?  ) and ( `pkAttr3`  =  ?  );
select `pkAttr0`, `pkAttr1`, `pkAttr2`, `pkAttr3`, `coAttr0_0`, `coAttr0_1` from `table0` where ( `coAttr0_0`  =  ?  ) and ( `coAttr0_1`  =  ?  )  ;
update `table0` set `coAttr0_0` = ?, `coAttr0_1` = ? where ( `pkAttr0`  =  ?  ) and ( `pkAttr1`  =  ?  ) and ( `pkAttr2`  =  ?  ) and ( `pkAttr3`  =  ?  );
select `pkAttr0`, `pkAttr1`, `pkAttr2`, `pkAttr3`, `coAttr0_0`, `coAttr0_1` from `table0` where ( `pkAttr0`  =  ?  ) or ( `pkAttr1`  =  ?  ) and ( `pkAttr2`  =  ?  ) and ( `pkAttr3`  =  ?  ) order by `pkAttr0`, `pkAttr1`, `pkAttr2`, `pkAttr3` ;
commit;