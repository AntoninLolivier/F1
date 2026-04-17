{% macro decode_f1_status(column_name) %}
    case {{ column_name }}
        when 'R' then 'Retired'
        when 'D' then 'Disqualified'
        when 'N' then 'Not Classified'
        when 'W' then 'Withdrawn'
        when 'F' then 'Failed to qualify'
        else {{ column_name }}
    end
{% endmacro %}