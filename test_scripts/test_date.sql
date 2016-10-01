connect c:\tpak;
set DEBUG_WHERE on;
select max(inj_month),min(inj_month),to_date('30-Nov-6') from recent_inj_data where inj_month > to_date('1-aug-6');
exit;
