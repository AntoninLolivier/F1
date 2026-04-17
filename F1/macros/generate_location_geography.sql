{% macro generate_location_geography(long, lat, alt) %}
    TO_GEOGRAPHY('POINTZ(' || {{ long }} || ' ' || {{ lat }} || ' ' || {{ alt }} || ')')
{% endmacro %}