#!/usr/bin/env sysbench
require("oltp_common")

function prepare_statements()
   if not sysbench.opt.skip_trx then
      prepare_begin()
      prepare_commit()
   end

   prepare_index_updates()
   prepare_non_index_updates()
   prepare_delete_inserts()
   prepare_order_ranges()
end

function event()
   if not sysbench.opt.skip_trx then
      begin()
   end

   execute_index_updates()
   execute_non_index_updates()
   execute_delete_inserts()

   execute_order_ranges()

   if not sysbench.opt.skip_trx then
      commit()
   end
end
