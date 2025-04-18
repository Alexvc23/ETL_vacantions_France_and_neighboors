
---------------------------------------------------------------------------------
-- Description: Creation of vacation calendar table for France and neighboring countries
-- Author: Alexis Valencia
-- Date: 2023-04-18
---------------------------------------------------------------------------------

CREATE TABLE t_vacances (
    vc_date      DATE    PRIMARY KEY,             -- one row per day

    fr_zone_a    BOOLEAN NOT NULL DEFAULT FALSE,  -- French academic zone A
    fr_zone_b    BOOLEAN NOT NULL DEFAULT FALSE,  -- French academic zone B
    fr_zone_c    BOOLEAN NOT NULL DEFAULT FALSE,  -- French academic zone C
    fr_zone_corse BOOLEAN NOT NULL DEFAULT FALSE, -- Corsica region

    allemagne    BOOLEAN NOT NULL DEFAULT FALSE,  -- Germany
    belgique     BOOLEAN NOT NULL DEFAULT FALSE,  -- Belgium
    espagne      BOOLEAN NOT NULL DEFAULT FALSE,  -- Spain
    italie       BOOLEAN NOT NULL DEFAULT FALSE,  -- Italy
    suisse       BOOLEAN NOT NULL DEFAULT FALSE,  -- Switzerland
    andorre      BOOLEAN NOT NULL DEFAULT FALSE,  -- Andorra
    monaco       BOOLEAN NOT NULL DEFAULT FALSE,  -- Monaco
    luxembourg   BOOLEAN NOT NULL DEFAULT FALSE,  -- Luxembourg

    /**
     * @constraint region_selection_check
     * @description Ensures at least one region is selected for vacation calendar entries.
     * @validates That at least one of the following boolean fields is true:
     *   - French academic zones (fr_zone_a, fr_zone_b, fr_zone_c, fr_zone_corse)
     *   - Neighboring countries (allemagne, belgique, espagne, italie, 
     *     suisse, andorre, monaco, luxembourg)
     */

    CHECK ( fr_zone_a OR fr_zone_b OR fr_zone_c OR fr_zone_corse
            OR allemagne OR belgique OR espagne OR italie
            OR suisse    OR andorre  OR monaco  OR luxembourg )

);

