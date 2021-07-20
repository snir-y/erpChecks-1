
qry_get_managers = """
        select distinct
            r.UserIndex,
            e.EmployeeEmail
        from IT_v_GetUserRoles r
        join Employees e on r.UserIndex = e.EmployeeUserIndex
        where UserRole = 8
        -- and FirstName != 'רונן שמר'
        """

qry_get_orders_with_bad_designer_mashlim = """
            select
                branchManagerName as מנהל,
                SaleProcessIndex as [תהליך מכירה],
                customerindex as [מספר לקוח],
                custName as [שם לקוח],
                PriceProposalNumerator as [מספר הזמנה],
                orderDesignerName as [מעצבת בהזמנה],
                producttype_name as [סוג מוצר],
                ppStatusName  as [סטטוס הזמנה]
            from
                dbo.IT_f_mashlimim_without_proper_designer(?)
            order by orderDesignerName
"""

qry_get_orders_with_old_est_supply_date = """
            select
                PriceProposalNumerator as [מספר הזמנה],
                BranchIndex as [מספר סניף],
                custname as [שם לקוח],
                ppStatusName as [סטטוס הזמנה],
                SupplyDate as [תאריך אספקה משוער],
                isnull(RequestedSeria,'') as [סריה מבוקשת],
                isnull(ManufactureSeria,'') as [סרית ייצור],
                isnull(SupplySeria,'') as [סרית אספקה מתוכננת],
                orderDesignerName as [מעצבת בהזמנה],
                GoremMeshalem as [גורם משלם],
                isnull(projectrforeignindex,'') as [מספר פרויקט],
                isnull(projectname,'') as [שם פרויקט]
            from
                [dbo].[IT_f_get_orders_with_old_supplydate](?)
"""

qry_get_orders_with_approaching_supplydate_and_no_requested_seria = """
            select
                PriceProposalNumerator as [מספר הזמנה],
                BranchIndex as [מספר סניף],
                custname as [שם לקוח],
                ppStatusName as [סטטוס הזמנה],
                SupplyDate as [תאריך אספקה משוער],
                isnull(RequestedSeria,'') as [סריה מבוקשת],
                orderDesignerName as [מעצבת בהזמנה],
                GoremMeshalem as [גורם משלם],
                isnull(projectrforeignindex,'') as [מספר פרויקט],
                isnull(projectname, '') as [שם פרויקט]
            from
            [dbo].[IT_f_get_orders_with_approaching_supplydate_and_no_requested_seria](?)
"""

qry_get_commited_task_alef_from_previous_day = """
    select
        a.PriceProposalNumerator as [מספר הזמנה],
        a.bitsuano as [מספר ביצוע],
        a.taskname  as [שם משימה] ,
        a.Commited as [בוצע],
        a.userInChargeName as [בוצע ע"י],
        a.CommitDate as [תאריך ביצוע] ,
        a.commitTime as [זמן ביצוע],
        a.custName as [שם לקוח],
        a.ppDesignerName as [שם מעצבת],
        a.BranchName as [סניף],
        a.RequestedSeria  as [סריה מבוקשת],
        a.ManufactureSeria as [סריה לייצור]
    from
        IT_f_get_commited_task_alef_from_previous_day(?, ?) a -- managerUserIndex index OR designerUserIndex
"""

qry_get_roles_in_manager_branch = """
    select
        EmployeeFirstName,
        EmployeeLastName,
        EmployeeMail,
        RoleName,
        RoleIndex,
        EmployeeUserIndex,
        Branchindex
    from IT_f_get_users_data_by_manager(?, ?) -- managerUserIndex, rolelist
"""
