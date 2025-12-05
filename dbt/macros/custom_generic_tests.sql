{% macro test_positive(model, column_name) %}
    -- Fails when value is <= 0
    select *
    from {{ model }}
    where {{ column_name }} <= 0
{% endmacro %}

{% macro test_between_zero_one(model, column_name) %}
    -- Fails when value is outside [0,1]
    select *
    from {{ model }}
    where {{ column_name }} < 0
       or {{ column_name }} > 1
{% endmacro %}

{% macro test_after_or_equal(model, column_name, compare_column) %}
    -- Fails when column is earlier than compare_column (both must be non-null)
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ compare_column }} is not null
      and {{ column_name }} < {{ compare_column }}
{% endmacro %}
