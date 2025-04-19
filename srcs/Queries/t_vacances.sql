
---------------------------------------------------------------------------------
-- Description: Creation of vacation calendar table for France and neighboring countries
-- Author: Alexis Valencia
-- Date: 2023-04-18
---------------------------------------------------------------------------------

CREATE TABLE t_vacances (
    vc_date      DATE    PRIMARY KEY,             -- one row per day
    fr_zone_a    NUMERIC(1) NOT NULL DEFAULT 0,  -- French academic zone A
    fr_zone_b    NUMERIC(1) NOT NULL DEFAULT 0,  -- French academic zone B
    fr_zone_c    NUMERIC(1) NOT NULL DEFAULT 0,  -- French academic zone C
    fr_zone_corse NUMERIC(1) NOT NULL DEFAULT 0, -- Corsica region

    allemagne    NUMERIC(1) NOT NULL DEFAULT 0,  -- Germany
    belgique     NUMERIC(1) NOT NULL DEFAULT 0,  -- Belgium
    espagne      NUMERIC(1) NOT NULL DEFAULT 0,  -- Spain
    italie       NUMERIC(1) NOT NULL DEFAULT 0,  -- Italy
    suisse       NUMERIC(1) NOT NULL DEFAULT 0,  -- Switzerland
    andorre      NUMERIC(1) NOT NULL DEFAULT 0,  -- Andorra
    monaco       NUMERIC(1) NOT NULL DEFAULT 0,  -- Monaco
    luxembourg   NUMERIC(1) NOT NULL DEFAULT 0,  -- Luxembourg

    /**
     * @constraint region_selection_check
     * @description Ensures at least one region is selected for vacation calendar entries.
     * @validates That at least one of the following boolean fields is true:
     *   - French academic zones (fr_zone_a, fr_zone_b, fr_zone_c, fr_zone_corse)
     *   - Neighboring countries (allemagne, belgique, espagne, italie, 
     *     suisse, andorre, monaco, luxembourg)
     */

    CHECK ( fr_zone_a > 0 OR fr_zone_b > 0 OR fr_zone_c > 0 OR fr_zone_corse > 0
            OR allemagne > 0 OR belgique > 0 OR espagne > 0 OR italie > 0
            OR suisse > 0 OR andorre > 1 OR monaco > 0 OR luxembourg > 0 )

);

