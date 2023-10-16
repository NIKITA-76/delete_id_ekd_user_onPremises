DO
$$
    DECLARE
        clusr          record;
        clusr_notfy    record;
        empl           record;
        empl_del       record;
        nc_temp2       record;
        clien_list     record;
        nc_temp        record;
        employee_index int    = 0;
        ekd_ekd        text   = 'oberon_ekd_ekd_3d44f3a21e414b8b92213efc00362668'; -- база ekd нужного тенанта
        ekd_ca         text   = 'oberon_ekd_ca_ecb2a12dc08c4b1fa9d8caf5dd9b764d'; --  база ca нужного тенанта
        empl_list      text[] = ARRAY ['1ba9ddef-d262-4b1f-9eef-e8e0b94add1a']; -- <<<< employee_id тех сотрудников, которых нужно удалить


    BEGIN

        FOR employee_index IN 1..array_length(empl_list, 1)
            LOOP


                SELECT *
                INTO clien_list
                FROM dblink(
                                 'dbname=' || ekd_ekd,
                                 'SELECT user_id FROM client_user
        JOIN employee e ON client_user.id = e.client_user_id
WHERE e.id IN (''' || empl_list[employee_index] || ''')'
                         ) AS remote_data(user_id uuid);
                RAISE NOTICE 'Начинаем работу с сотрудником employee_id >>>>>>>> % <<<<<<<< employee_id', empl_list[employee_index];

                IF EXISTS (SELECT 1
                           FROM dblink(
                                                'dbname=''' || ekd_ekd || '''',
                                                'SELECT 1 FROM document_signer ds WHERE ds.employee_id = ''' ||
                                                empl_list[employee_index] || ''''
                                    ) AS result(row_exists integer)) THEN
                    RAISE EXCEPTION 'У этого сотрудника имеются записи в document_signer, employee_id сотрудника -- %', clien_list.user_id;
                END IF;


                PERFORM dblink('dbname= ''' || ekd_ca || ''' ',
                               'DELETE FROM nqes_issue_sms_confirmation_attempt nisca
                               WHERE nisca.nqes_issue_sms_confirmation_id in (SELECT nisc.id
                                                                               FROM nqes_issue_sms_confirmation nisc
                                                                               WHERE nisc.nqes_issue_task_id in (
                                                                               select nit.id
                                                                               from nqes_issue_task nit
                                                                               WHERE nit.user_id = ''' ||
                               clien_list.user_id ||
                               '''))');
                RAISE NOTICE 'user_id >>>>>>>> % <<<<<<<< user_id', clien_list.user_id;
                RAISE NOTICE 'Удалили nqes_issue_sms_confirmation_attempt у физ.лица: %', clien_list.user_id;
                PERFORM dblink('dbname= ''' || ekd_ca || ''' ',
                               'DELETE FROM nqes_issue_sms_confirmation nisc
                               WHERE nisc.nqes_issue_task_id = (SELECT id FROM nqes_issue_task nit
                                                                   WHERE nit.user_id = ''' || clien_list.user_id ||
                               ''')');
                RAISE NOTICE 'Удалили nqes_issue_sms_confirmation у физ.лица: %', clien_list.user_id;

                PERFORM dblink('dbname= ''' || ekd_ca || ''' ',
                               'DELETE FROM nqes_issue_task nit
                                 WHERE nit.user_id = ''' || clien_list.user_id || '''');
                RAISE NOTICE 'Удалили nqes_issue_task у физ.лица: %', clien_list.user_id;


                FOR clusr IN (SELECT *
                              FROM dblink(
                                                   'dbname= ''' || ekd_ekd || '''',
                                                   'SELECT user_id, id FROM client_user WHERE user_id = ''' ||
                                                   clien_list.user_id ||
                                                   ''''
                                       )
                                       AS remote_data(user_id uuid, id uuid))
                    LOOP

                        FOR empl IN (SELECT *
                                     FROM dblink(
                                                          'dbname= ''' || ekd_ekd || '''',
                                                          'SELECT id FROM employee WHERE client_user_id = ''' ||
                                                          clusr.id || ''''
                                              )
                                              AS remote_data(id uuid))
                            LOOP


                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM legal_entity_employee_role WHERE employee_id = ''' ||
                                               empl.id || '''');
                                RAISE NOTICE 'Удалили legal_entity_employee_role у сотрудника: %', empl.id;

                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM permitted_client_department WHERE employee_id = ''' ||
                                               empl.id || '''');
                                RAISE NOTICE 'Удалили legal_entity_employee_role у сотрудника: %', empl.id;

                                PERFORM dblink('dbname= ''' || ekd_ekd || ''' ',
                                               'DELETE FROM update_watcher_department_ids_on_documents_task uwdiod
                                               WHERE uwdiod.id = ''' || empl.id || '''');
                                RAISE NOTICE 'Удалили update_watcher_department_ids_on_documents_task у сотрудника: %', empl.id;

                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM permitted_document_type WHERE employee_id = ''' ||
                                               empl.id || '''');
                                RAISE NOTICE 'Удалили permitted_document_type у сотрудника: %', empl.id;

                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM permitted_application_type WHERE employee_id = ''' ||
                                               empl.id || '''');
                                RAISE NOTICE 'Удалили permitted_application_type у сотрудника: %', empl.id;


                            END LOOP;

                        FOR empl_del IN (SELECT *
                                         FROM dblink(
                                                              'dbname= ''' || ekd_ekd || '''',
                                                              'SELECT id FROM employee WHERE client_user_id = ''' ||
                                                              clusr.id || ''''
                                                  )
                                                  AS remote_data(id uuid))
                            LOOP
                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM employee WHERE id = ''' || empl_del.id || '''');
                                RAISE NOTICE 'Удалили employee у сотрудника: %', empl_del.id;
                            END LOOP;


                        FOR clusr_notfy IN (SELECT *
                                            FROM dblink('dbname= ''' || ekd_ekd || '''',
                                                        'SELECT id FROM client_user_notification_setting WHERE client_user_id = ''' ||
                                                        clusr.id || '''') AS remote_data(id uuid))
                            LOOP
                                PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                               'DELETE FROM client_user_sent_notification WHERE notification_setting_id = ''' ||
                                               clusr_notfy.id || '''');
                                RAISE NOTICE 'Удалили employee у сотрудника: %', empl.id;
                            END LOOP;


                        PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                       'DELETE FROM client_user_notification_setting WHERE client_user_id = ''' ||
                                       clusr.id || '''');
                        RAISE NOTICE 'Удалили client_user_notification_setting у физ.лица: %', clusr.id;
                        PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                       'DELETE FROM client_user WHERE user_id = ''' || clien_list.user_id || '''');
                        RAISE NOTICE 'Удалили employee у физ.лица: %', clien_list;

                        PERFORM dblink('dbname= ''' || ekd_ekd || '''',
                                       'DELETE FROM user_reference WHERE id = ''' || clien_list.user_id || '''');
                        RAISE NOTICE 'Удалили employee у физ.лица: %', clien_list;
                    END LOOP;
                FOR nc_temp IN (SELECT nc.id       as ncc,
                                       u.id        as uidd,
                                       pd.id       as pdd,
                                       otp.id      as otp,
                                       ui.id       as ui,
                                       ui_1.id     as ui_1,
                                       ui_2.id     as ui_2,
                                       udt.id      as udt,
                                       wotp.id     as wotp,
                                       rpr.id      as rpr,
                                       stteoi.id   as stteoi,
                                       stteoi_1.id as stteoi_1,
                                       uvt.id      as uvt,
                                       uex.id      as uex,
                                       stteoi_2.id as stteoi_2
                                FROM public.user u
                                         LEFT JOIN user_login ul ON ul.user_id = u.id
                                         LEFT JOIN user_invitation ui ON ui.inviter_id = u.id
                                         LEFT JOIN user_invitation ui_1 ON ui_1.user_id = u.id
                                         LEFT JOIN user_invitation ui_2 ON ui_2.user_login_id = ul.id
                                         LEFT JOIN user_disable_task udt on udt.user_id = u.id
                                         LEFT JOIN user_verification_token uvt ON uvt.user_id = u.id
                                         LEFT JOIN notification_channel nc ON nc.user_login_id = ul.id
                                         LEFT JOIN signing_type_to_enable_on_invite ON nc.user_login_id = ul.id
                                         LEFT JOIN one_time_password otp ON otp.user_login_id = ul.id
                                         LEFT JOIN signing_type_to_enable_on_invite stteoi
                                                   ON stteoi.user_invitation_id = ui.id
                                         LEFT JOIN signing_type_to_enable_on_invite stteoi_1
                                                   ON stteoi_1.user_invitation_id = ui_1.id
                                         LEFT JOIN signing_type_to_enable_on_invite stteoi_2
                                                   ON stteoi_2.user_invitation_id = ui_2.id
                                         LEFT JOIN reset_password_request rpr ON rpr.user_id = u.id
                                         LEFT JOIN wrong_one_time_password_attempt wotp
                                                   ON wotp.one_time_password_id = otp.id
                                         LEFT JOIN person p ON u.id = p.user_id
                                         LEFT JOIN person_document pd ON p.id = pd.person_id
                                         LEFT JOIN user_external_id uex ON uex.user_id = u.id
                                WHERE u.id = clien_list.user_id)
                    LOOP
                        DELETE FROM one_time_password WHERE id = nc_temp.otp;
                        RAISE NOTICE 'Удалили one_time_password: %', nc_temp.otp;

                        DELETE FROM wrong_one_time_password_attempt WHERE id = nc_temp.wotp;
                        RAISE NOTICE 'Удалили wrong_one_time_password: %', nc_temp.wotp;

                        DELETE FROM wrong_one_time_password_attempt WHERE id = nc_temp.wotp;
                        RAISE NOTICE 'Удалили wrong_one_time_password: %', nc_temp.wotp;

                        DELETE FROM signing_type_to_enable_on_invite WHERE id = nc_temp.stteoi_1;
                        RAISE NOTICE 'Удалили wrong_one_time_password: %', nc_temp.stteoi_1;

                        DELETE FROM signing_type_to_enable_on_invite WHERE id = nc_temp.stteoi;
                        RAISE NOTICE 'Удалили wrong_one_time_password: %', nc_temp.stteoi;

                        DELETE FROM signing_type_to_enable_on_invite WHERE id = nc_temp.stteoi_2;
                        RAISE NOTICE 'Удалили signing_type_to_enable_on_invite: %', nc_temp.stteoi_2;

                        DELETE FROM user_external_id WHERE id = nc_temp.uex;
                        RAISE NOTICE 'Удалили user_external_id: %', nc_temp.uex;

                        DELETE FROM user_invitation WHERE id = nc_temp.ui;
                        RAISE NOTICE 'Удалили user_invitation: %', nc_temp.ui;

                        DELETE FROM user_invitation WHERE id = nc_temp.ui_1;
                        RAISE NOTICE 'Удалили user_invitation: %', nc_temp.ui_1;

                        DELETE FROM user_disable_task WHERE id = nc_temp.udt;
                        RAISE NOTICE 'Удалили user_disable_task: %', nc_temp.udt;

                        DELETE FROM user_verification_token WHERE id = nc_temp.uvt;
                        RAISE NOTICE 'Удалили user_verification_token: %', nc_temp.uvt;

                        DELETE FROM notification_channel WHERE id = nc_temp.ncc;
                        RAISE NOTICE 'Удалили notification_channel: %', nc_temp.ncc;

                        DELETE FROM person_document WHERE id = nc_temp.pdd;
                        RAISE NOTICE 'Удалили person_document: %', nc_temp.pdd;


                    END LOOP;

                FOR nc_temp2 IN (SELECT p.id as pid
                                 FROM person p
                                 WHERE p.user_id = clien_list.user_id)
                    LOOP
                        PERFORM dblink('dbname= ''' || ekd_ca || ''' ',
                                       'DELETE FROM nqes_issue_request nir
                                         WHERE nir.person_id = ''' || nc_temp2.pid || '''');

                        DELETE FROM user_login WHERE user_id = nc_temp.uidd;
                        RAISE NOTICE 'Удалили user_login: %', nc_temp.uidd;

                        DELETE FROM person WHERE id = nc_temp2.pid;
                        RAISE NOTICE 'Удалили person: %', nc_temp2.pid;
                    END LOOP;
                DELETE FROM public.user WHERE id = clien_list.user_id;
                RAISE NOTICE 'Удалили user: %', clien_list.user_id;
            END LOOP;

    exception
        when others then
            if SQLERRM like
               '%on table "person_certificate"%' then
                raise exception 'У этого сотрудника есть связь в person_certificate, id физ. лица  -- %', clien_list;

            elsif SQLERRM like '%on table "document"%' then
                raise exception 'У этого сотрудника есть связи с документами, id сотрудника  -- %', empl.id;

            elsif SQLERRM like '%on table "document"%' then
                raise exception 'У этой записи client_user есть связи с документами, id client_user  -- %', clusr.id;

            elsif SQLERRM like '%on table "application"%' then
                raise exception 'У этого сотрудника есть связи с документами, id сотрудника  -- %', empl.id;

            elsif SQLERRM like '%on table "application"%' then
                raise exception 'У этой записи client_user есть связи с заявлениями, id client_user  -- %', clusr.id;

            elsif SQLERRM like
                  '%on table "employee_dismiss_task"%' then
                raise exception 'У этого сотрудника есть связь в on table "employee_dismiss_task", id сотрудника  -- %', empl.id;

            elsif SQLERRM like
                  '%on table "employee_tag"%' then
                raise exception 'У этого сотрудника есть связь в on table "application_client_department", id сотрудника  -- %', empl.id;
            else
                raise;
            end if;
    END
$$;
