{#
    This macro returns the description of the pickup_datetime 
#}

{% macro get_quater(pickup_datetime_month) -%}

    case {{ dbt.safe_cast("payment_pickup_datetime_monthtype", api.Column.translate_type("integer")) }}  
        when 1 then '1'
        when 2 then '1'
        when 3 then '1'
        when 4 then '2'
        when 5 then '2'
        when 6 then '2'
        when 7 then '3'
        when 8 then '3'
        when 9 then '3'
        when 10 then '4'
        when 11 then '4'
        when 12 then '4'
    end

{%- endmacro %}