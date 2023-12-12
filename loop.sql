-- SELECT * FROM organization

DO $$
DECLARE
    name_org organization.name_org%TYPE;
    category_org organization.category_org%TYPE;
BEGIN
    FOR i IN 1..20
    LOOP
        name_org := 'org_' || i;
        category_org := 'category_' || floor(random() * 10 + 1);

        INSERT INTO organization (name_org, category_org)
        VALUES (name_org, category_org);
    END LOOP;
END;
$$
