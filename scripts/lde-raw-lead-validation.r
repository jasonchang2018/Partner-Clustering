
library(opploansanalytics)
load.packages()

getAdmethodList = function () {
    
    queryReporting(
    "
    select
        admethod
        , count(*) as volume
    from
        public.all_allapps
    where
        refi = 'N'
        and left(denygrp,1) > '2'
        and appldate >= '2020-01-01'::date
        and appldate < '2020-02-01'::date
    group by
        1
    order by
        2 desc
    "
    )
}

getPayloads = function (admethod, start.date = '2020-01-01', end.date = '2020-02-01', limit = 100) {
    
    queryReporting(paste0(
    "
    select
        lde.lead_id
        , lde.lead_time at time zone 'America/Chicago' as lead_time
        , c_app.name
        , c_app.contact
        , c_ofl.email
        , c_app.createddate at time zone 'America/Chicago' as appldate
        , c_am.name as admethod
        , c_am.external_id as partnerid
        , lde.raw_lead
    from
        lde4.leads as lde
        inner join
            cloudlending.advertising_method as c_am
            on lde.partnerid = c_am.external_id
        left join
            cloudlending.applications as c_app
            on lde.lead_id = c_app.lde4_lead_id
        left join
            cloudlending.contact as c_ofl
            on c_app.contact = c_ofl.id
    where
        c_am.name = '", admethod, "'
        and lde.lead_time at time zone 'America/Chicago' >= '", start.date, "'::date
        and lde.lead_time at time zone 'America/Chicago' < '", end.date, "'::date
    order by
        random()
    limit ", limit
    ))
    
}

getRegInputs = function (admethod, start.date = '2020-01-01', end.date = '2020-02-01', limit = NA) {
        
    raw.query = querySnowflake(paste0(
        "
        select
            reg.event_id
            , reg.session_id
            , reg.user_id
            , users_.\"identity\" as contact
            , reg.\"time\"
            , reg.email
            , reg.partner_id
            , reg.neededinputs
        from
            heap_prod.heap._viewed_lead_registration_ as reg
            inner join
                ods_prod.cloudlending.advertising_method__c as c_am
                on reg.partner_id = c_am.external_id__c
                and c_am.name = '", admethod, "'
            left join
                heap_prod.heap.users as users_
                on reg.user_id = users_.user_id
        where
            reg.\"time\" >= '", start.date, "'::date
            and reg.\"time\" < '", end.date, "'::date
        ", ifelse(is.na(limit), "", paste0("limit ", limit))
    ))
    colnames(raw.query) %<>% str_to_lower()
    
    
    raw.query %>%
        mutate(
            fields = neededinputs %>%
                map(
                    .f = function (x) {
                        x %>%
                            str_match_all(
                                pattern = regex('registration\\[(.*?)\\]')
                            ) %>%
                            .[[1]] %>% .[ ,2]
                    }
                )
        )
    
    
}

# setGlobalVars = function () {

#     setEvaluationVariables = function () {



#         ##  Does the proposed payload contain any objects NOT expected?  ##
#         expected.keys <<- c(
#             'isProduction',
#             'leadOfferId',
#             'language',
#             'socialSecurityNumber',
#             'email',
#             'stateCode',
#             'grossMonthlyIncome',
#             'currency',
#             'firstName',
#             'lastName',
#             'dateOfBirth',
#             'streetAddress',
#             'city',
#             'zip',
#             'countryCode',
#             'mobilePhone',
#             'homePhone',
#             'bankName',
#             'abaRoutingNumber',
#             'accountNumber',
#             'accountType',
#             'accountLength',
#             'incomeType',
#             'payrollType',
#             'payrollFrequency',
#             'lastPayrollDate',
#             'nextPayrollDate',
#             'employerName',
#             'hireDate',
#             'requestedLoanAmount'
#         )
#         expected.objects <<- c(
#             'personalInfo',
#             'address',
#             'bankInfo',
#             'incomeInfo',
#             'employmentInfo'
#         )


#         ##  What are the key value pairs?  ##
# #         values <<- '\\{?\\\".*?\\":\\s(?!=\\{)([^\\{]*?)[,\\}]'   ###
# #         values <<- '\\\"[^\\{]*?\\\":\\s(?:(\\\"[^\\{]*?\\\")|([^\\{]*?)[,\\}])'
#         values <<- '\\\"[^\\{]*?\\\":\\s(?:(\\\"[^\\{]*?\\\")|([^\\{]*?)[,\\}])'
#         keys <<- '\\\"([^,]*?)\\\":'


#         ##  What data types do we expect?  ##

#         #   Always Bad   #
#         quoted.null <<- '^\\\"null\\\"$'
#         quoted.empty <<- '^\\\"(?:\\s+)?\\\"$'
#         quoted.boolean <<- '^\\\"(?:true|false)\\\"$'

#         quoted.string <<- '^\\\".*\\\"$'
#         quoted.numeric <<- '^\\\"\\d+\\\"$'
#         quoted.zip <<- '^\\\"\\d{5}\\\"$'
#         quoted.date <<- '^\\\"\\d{8}\\\"$'
#         quoted.aba <<- '^\\\"\\d{9}\\\"$'
#         quoted.ssn <<- '^\\\"(?:\\d{3})-?(?:\\d{2})-?(?:\\d{4})\\\"$'
#         quoted.ssn.aba <<- '^\\\"\\d{9}\\\"$'
#         quoted.phone <<- '^\\\"\\+?\\d{10,11}\\\"$'
#         quoted.name <<- '^\\\"[a-zA-Z\\s\\-\\.]+\\\"$'
#     #     quoted.email <<- '^\\\".+@.+\\.\\w+\\\"$'
# #         quoted.email <<- '^\\\"(?:[a-z0-9!#$%&\\\'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&\\\'*+/=?^_`{|}~-]+)*|\\\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\\\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])\\\"$'
#         quoted.email <<- '^\\\".*@.*\\..*\\\"$'
#         quoted.state <<- '^\\\"[A-Z]{2}\\\"$'
#     #     quoted.state <<- '^\\\"(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\\\"$'
#         quoted.currency <<- '^\\\"USD\\\"$'
#         quoted.currencyCode <<- '^\\\"USD\\\"$'
#         quoted.blank <<- '^\\\"[\\s]*\\\"$'
#         quoted.countryCode <<- '^\\\"\\s*(?:US|USA)\\s*\\\"$'
#         quoted.accountNumber <<- '^\\\"\\d{6,17}\\\"$'

#         unquoted.numeric <<- '[\\d\\.]+(?![\\\"@\\w\\s\\-])'
#         unquoted.float.positive <<- '([^\\-\\s\\\"]\\d+\\.?\\d*[^\\\"\\,])'
#         unquoted.decimal <<- '(?:\\d+\\.\\d+)+(?![\\\"@\\w\\s\\-])'
#         unquoted.integer <<- '(\\d+)'
#         unquoted.boolean <<- '(?<!\\\")(?:true|false)(?!=\\\")'
#         unquoted.null <<- '(?<!\\\")null(?!=\\\")'

#         optional.quoted.float.positive <<- '(\\\"?[^\\-\\s]\\d+\\.?\\d*\\\"?)'

# #         expect.quoted.string <<- c(
# #             'campaignId',
# #             'leadOfferId',
# #             'streetAddress',
# #             'bankName'
# #         )

# #         expect.quoted.employerName <<- c(
# #             'employerName'
# #         )

# #         expect.quoted.numeric <<- c(
# #             'accountNumber'
# #         )

# #         expect.quoted.name <<- c(
# #             'firstName',
# #             'lastName',
# #             'city',
# #             'language',
# #             'countryCode',
# #             'incomeType',
# #             'payrollType'
# #         )

# #         expect.quoted.currency <<- c(
# #             'currency'
# #         )

# #         expect.quoted.zip <<- c(
# #             'zip'
# #         )

# #         expect.quoted.date.past <<- c(
# #             'dateOfBirth',
# #             'lastPayrollDate',
# #             'hireDate'
# #         )

# #         expect.quoted.date.future <<- c(
# #             'nextPayrollDate'
# #         )

# #         expect.quoted.ssn.aba <<- c(
# #             'socialSecurityNumber',
# #             'abaRoutingNumber'
# #         )

# #         expect.quoted.phone <<- c(
# #             'mobilePhone',
# #             'homePhone'
# #         )

# #         expect.quoted.email <<- c(
# #             'email'
# #         )

# #         expect.quoted.state <<- c(
# #             'stateCode'
# #         )

# #         expect.unquoted.boolean <<- c(
# #             'isProduction'
# #         )

# #         expect.unquoted.float.positive <<- c(
# #             'grossMonthlyIncome'
# #         )

# #         expect.unquoted.integer <<- c(
# #             'accountType',
# #             'accountLength',
# #             'payrollFrequency'
# #         )

# #         expect.optional.quoted.float.positive <<- c(
# #             'requestedLoanAmount'
# #         )

# }
#     setPartnerMappingLDE = function () {
#         queryReporting(
#         "
#         select
#             external_id
#             , name
#             , control_file
#             , case 	when control_file = 'Leads02' then 'Non-Affiliate'
#                     when control_file = 'Opp4' then 'Affiliate'
#                     when control_file = 'Opp23' then 'Lite'
#                     else 'Else'
#                     end as lde_type
#         from
#             cloudlending.advertising_method
#         "
#         )
# }

#     setEvaluationVariables()
#     partner.map <<- setPartnerMappingLDE()
    
# }

setEvaluationVariables = function () {

    expected.keys = c(
        'isProduction',
        'leadOfferId',
        'language',
        'socialSecurityNumber',
        'email',
        'stateCode',
        'grossMonthlyIncome',
        'currency',
        'firstName',
        'lastName',
        'dateOfBirth',
        'streetAddress',
        'city',
        'zip',
        'countryCode',
        'mobilePhone',
        'homePhone',
        'bankName',
        'abaRoutingNumber',
        'accountNumber',
        'accountType',
        'accountLength',
        'incomeType',
        'payrollType',
        'payrollFrequency',
        'lastPayrollDate',
        'nextPayrollDate',
        'employerName',
        'hireDate',
        'requestedLoanAmount'
    )
    expected.objects = c(
        'personalInfo',
        'address',
        'bankInfo',
        'incomeInfo',
        'employmentInfo'
    )
    expected.value.ranges = data.frame(
        key = c(
            'language',
            'incomeType',
            'payrollType',
            'payrollFrequency',
            'zip'
        ), 
        value = I(list(
            c('en'),
            c('Employment', 'OtherNonTaxableIncome', 'OtherTaxableIncome', 'SelfEmployed'),
            c('Cash', 'DirectDeposit', 'PaperCheck'),
            c(1,2,3,4),
            suppressWarnings({suppressMessages({read_csv("..\\data\\zip_code_database.csv")})}) %>% filter(country == 'US') %>% .$zip
        )),
        stringsAsFactors = FALSE
    )
    
    
    values = '\\\"[^\\{]*?\\\":\\s(?:(\\\"[^\\{]*?\\\")|([^\\{]*?)[,\\}])'
    keys = '\\\"([^,]*?)\\\":'


    quoted.null = '^\\\"null\\\"$'
    quoted.empty = '^\\\"(?:\\s+)?\\\"$'
    quoted.boolean = '^\\\"(?:true|false)\\\"$'
    quoted.string = '^\\\".*\\\"$'
    quoted.numeric = '^\\\"\\d+\\\"$'
    quoted.zip = '^\\\"\\d{5}\\\"$'
    quoted.date = '^\\\"\\d{8}\\\"$'
    quoted.aba = '^\\\"\\d{9}\\\"$'
    quoted.ssn = '^\\\"(?:\\d{3})-?(?:\\d{2})-?(?:\\d{4})\\\"$'
    quoted.ssn.aba = '^\\\"\\d{9}\\\"$'
    quoted.phone = '^\\\"\\+?\\d{10,11}\\\"$'
    quoted.name = '^\\\"[a-zA-Z\\s\\-\\.]+\\\"$'
    quoted.email = '^\\\".*@.*\\..*\\\"$'
    quoted.state = '^\\\"[A-Z]{2}\\\"$'
    quoted.currency = '^\\\"USD\\\"$'
    quoted.currencyCode = '^\\\"USD\\\"$'
    quoted.blank = '^\\\"[\\s]*\\\"$'
    quoted.countryCode = '^\\\"\\s*(?:US|USA)\\s*\\\"$'
    quoted.accountNumber = '^\\\"\\d{6,17}\\\"$'

    unquoted.numeric = '[\\d\\.]+(?![\\\"@\\w\\s\\-])'
    unquoted.float.positive = '([^\\-\\s\\\"]\\d+\\.?\\d*[^\\\"\\,])'
    unquoted.decimal = '(?:\\d+\\.\\d+)+(?![\\\"@\\w\\s\\-])'
    unquoted.integer = '(\\d+)'
    unquoted.boolean = '(?<!\\\")(?:true|false)(?!=\\\")'
    unquoted.null = '(?<!\\\")null(?!=\\\")'

    optional.quoted.float.positive = '(\\\"?[^\\-\\s]\\d+\\.?\\d*\\\"?)'
    
    
    
    list(
        expected.keys = expected.keys,
        expected.objects = expected.objects,
        values = values,
        keys = keys,
        quoted.null = quoted.null,
        quoted.empty = quoted.empty,
        quoted.boolean = quoted.boolean,
        quoted.string = quoted.string,
        quoted.numeric = quoted.numeric,
        quoted.zip = quoted.zip,
        quoted.date = quoted.date,
        quoted.aba = quoted.aba,
        quoted.ssn = quoted.ssn,
        quoted.ssn.aba = quoted.ssn.aba,
        quoted.phone = quoted.phone,
        quoted.name = quoted.name,
        quoted.email = quoted.email,
        quoted.state = quoted.state,
        quoted.currency = quoted.currency,
        quoted.currencyCode = quoted.currencyCode,
        quoted.blank = quoted.blank,
        quoted.countryCode = quoted.countryCode,
        quoted.accountNumber = quoted.accountNumber,
        unquoted.numeric = unquoted.numeric,
        unquoted.float.positive = unquoted.float.positive,
        unquoted.decimal = unquoted.decimal,
        unquoted.integer = unquoted.integer,
        unquoted.boolean = unquoted.boolean,
        unquoted.null = unquoted.null,
        optional.quoted.float.positive = optional.quoted.float.positive   
    )

}
setPartnerMappingLDE = function () {
    
    queryReporting(
    "
    select
        external_id
        , name
        , control_file
        , case 	when control_file = 'Leads02' then 'Non-Affiliate'
                when control_file = 'Opp4' then 'Affiliate'
                when control_file = 'Opp23' then 'Lite'
                else 'Else'
                end as lde_type
    from
        cloudlending.advertising_method
    "
    )
    
}

validateStructure = function (admethod, keys.missing, partner.map, ...) {
    
    args = list(...)
    validator = function (admethod) {
        
        if ( partner.map %>% filter(name == admethod) %>% .$lde_type %in% c('Affiliate') ) {
            
            ###  AFFILIATE  ###
            ldevalidate_structure_isProduction <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Affl
            ldevalidate_structure_leadOfferId <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Affl
            ldevalidate_structure_clickId <- function (keys.missing) {
                return (200)
            }                   ## DONE - LDE Affl
            ldevalidate_structure_campaignId <- function (keys.missing) {
                return (200)
            }                ## DONE - LDE Affl
            ldevalidate_structure_language <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Affl
            ldevalidate_structure_socialSecurityNumber <- function (keys.missing) {
                return (200)
            }      ## DONE - LDE Affl
            ldevalidate_structure_email <- function (keys.missing) {
                return (200)
            }                     ## DONE - LDE Affl
            ldevalidate_structure_stateCode <- function (keys.missing) {
                return (200)
            }                 ## DONE - LDE Affl
            ldevalidate_structure_grossMonthlyIncome <- function (keys.missing) {
                return (200)
            }        ## DONE - LDE Affl
            ldevalidate_structure_currency <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Affl
            ldevalidate_structure_currencyCode <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Affl
            ldevalidate_structure_personalInfo <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Affl
            ldevalidate_structure_firstName <- function (keys.missing) {
                return (200)
            }                 ## DONE - LDE Affl
            ldevalidate_structure_lastName <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Affl
            ldevalidate_structure_dateOfBirth <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Affl
            ldevalidate_structure_address <- function (keys.missing) {
                return (200)
            }                   ## DONE - LDE Affl
            ldevalidate_structure_streetAddress <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Affl
            ldevalidate_structure_city <- function (keys.missing) {
                return (200)
            }                      ## DONE - LDE Affl
            ldevalidate_structure_zip <- function (keys.missing) {
                return (200)
            }                       ## DONE - LDE Affl
            ldevalidate_structure_countryCode <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Affl
            ldevalidate_structure_mobilePhone <- function (keys.missing) {

                present.personalInfo = expr(! 'personalInfo' %in% keys.missing)
                missing.mobilePhone = expr('mobilePhone' %in% keys.missing)

                if (  eval(present.personalInfo & missing.mobilePhone) )
                    return (701)
                if (! eval(present.personalInfo & missing.mobilePhone) )
                    return (200)

            }               ## DONE - LDE Affl
            ldevalidate_structure_homePhone <- function (keys.missing) {
                return (200)
            }                 ## DONE - LDE Affl
            ldevalidate_structure_bankInfo <- function (keys.missing) {

                missing.bankInfo = expr('bankInfo' %in% keys.missing)

                if (  eval(missing.bankInfo) )
                    return (709)
                if (! eval(missing.bankInfo) )
                    return (200)

            }                  ## DONE - LDE Affl
            ldevalidate_structure_bankName <- function (keys.missing) {

                present.bankInfo = expr(! 'bankInfo' %in% keys.missing)
                missing.bankName = expr('bankName' %in% keys.missing)

                if (  eval(present.bankInfo & missing.bankName) )
                    return (709)
                if (! eval(present.bankInfo & missing.bankName) )
                    return (200)

            }                  ## DONE - LDE Affl
            ldevalidate_structure_abaRoutingNumber <- function (keys.missing) {
                return (200)
            }          ## DONE - LDE Affl
            ldevalidate_structure_accountNumber <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Affl
            ldevalidate_structure_accountType <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Affl
            ldevalidate_structure_accountLength <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Affl
            ldevalidate_structure_incomeInfo <- function (keys.missing) {

                missing.incomeInfo = expr('incomeInfo' %in% keys.missing)

                if (  eval(missing.incomeInfo) )
                    return (704)
                if (! eval(missing.incomeInfo) )
                    return (200)

            }                ## DONE - LDE Affl
            ldevalidate_structure_incomeType <- function (keys.missing) {

                present.incomeInfo = expr(! 'incomeInfo' %in% keys.missing)
                missing.incomeType = expr('incomeType' %in% keys.missing)

                if (  eval(present.incomeInfo & missing.incomeType) )
                    return (704)
                if (! eval(present.incomeInfo & missing.incomeType) )
                    return (200)

            }                ## DONE - LDE Affl
            ldevalidate_structure_payrollType <- function (keys.missing) {

                present.incomeInfo = expr(! 'incomeInfo' %in% keys.missing)
                missing.payrollType = expr('payrollType' %in% keys.missing)

                if (  eval(present.incomeInfo & missing.payrollType) )
                    return (705)
                if (! eval(present.incomeInfo & missing.payrollType) )
                    return (200)

            }               ## DONE - LDE Affl
            ldevalidate_structure_payrollFrequency <- function (keys.missing) {
                return (200)
            }          ## DONE - LDE Affl
            ldevalidate_structure_lastPayrollDate <- function (keys.missing) {
                return (200)
            }           ## DONE - LDE Affl
            ldevalidate_structure_nextPayrollDate <- function (keys.missing) {
                return (200)
            }           ## DONE - LDE Affl
            ldevalidate_structure_employmentInfo <- function (keys.missing) {

                missing.employmentInfo = expr('employmentInfo' %in% keys.missing)

                if (  eval(missing.employmentInfo) )
                    return (702)
                if (! eval(missing.employmentInfo) )
                    return (200)

            }            ## DONE - LDE Affl
            ldevalidate_structure_employerName <- function (keys.missing) {

                present.employmentInfo = expr(! 'employmentInfo' %in% keys.missing)
                missing.employerName = expr('employerName' %in% keys.missing)

                if (  eval(present.employmentInfo & missing.employerName) )
                    return (702)
                if (! eval(present.employmentInfo & missing.employerName) )
                    return (200)

            }              ## DONE - LDE Affl
            ldevalidate_structure_hireDate <- function (keys.missing) {

                present.employmentInfo = expr(! 'employmentInfo' %in% keys.missing)
                missing.hireDate = expr('hireDate' %in% keys.missing)

                if (  eval(present.employmentInfo & missing.hireDate) )
                    return (703)
                if (! eval(present.employmentInfo & missing.hireDate) )
                    return (200)

            }                  ## DONE - LDE Affl
            ldevalidate_structure_requestedLoanAmount <- function (keys.missing) {
                return (200)
            }       ## DONE - LDE Affl
            ldevalidate_structure_loanPurpose <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Affl
            
        } else {
    
            ###  LDE LITE, LDE NON-AFFILIATE  ###
            ldevalidate_structure_isProduction <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Lite
            ldevalidate_structure_leadOfferId <- function (keys.missing) {

                missing.leadOfferId = expr('leadOfferId' %in% keys.missing)

                if (  eval(missing.leadOfferId))
                    return (317)
                if (! eval(missing.leadOfferId))
                    return (200)

            }               ## DONE - LDE Lite
            ldevalidate_structure_clickId <- function (keys.missing) {
                return (200)
            }                   ## DONE - LDE Lite
            ldevalidate_structure_campaignId <- function (keys.missing) {
                return (200)
            }                ## DONE - LDE Lite
            ldevalidate_structure_language <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Lite
            ldevalidate_structure_socialSecurityNumber <- function (keys.missing) {

                missing.socialSecurityNumber = expr('socialSecurityNumber' %in% keys.missing)

                if (  eval(missing.socialSecurityNumber))
                    return (314)
                if (! eval(missing.socialSecurityNumber))
                    return (200)

            }      ## DONE - LDE Lite
            ldevalidate_structure_email <- function (keys.missing) {

                missing.email = expr('email' %in% keys.missing)

                if (  eval(missing.email))
                    return (312)
                if (! eval(missing.email))
                    return (200)

            }                     ## DONE - LDE Lite
            ldevalidate_structure_stateCode <- function (keys.missing) {

                missing.stateCode = expr('stateCode' %in% keys.missing)

                if (  eval(missing.stateCode))
                    return (311)
                if (! eval(missing.stateCode))
                    return (200)

            }                 ## DONE - LDE Lite
            ldevalidate_structure_grossMonthlyIncome <- function (keys.missing) {

                missing.grossMonthlyIncome = expr('grossMonthlyIncome' %in% keys.missing)

                if (  eval(missing.grossMonthlyIncome))
                    return (332)
                if (! eval(missing.grossMonthlyIncome))
                    return (200)

            }        ## DONE - LDE Lite
            ldevalidate_structure_currency <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Lite
            ldevalidate_structure_currencyCode <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Lite
            ldevalidate_structure_personalInfo <- function (keys.missing) {

                missing.personalInfo = expr('personalInfo' %in% keys.missing)

                if (  eval(missing.personalInfo))
                    return (341)
                if (! eval(missing.personalInfo))
                    return (200)

            }              ## DONE - LDE Lite
            ldevalidate_structure_firstName <- function (keys.missing) {

                present.personalInfo = expr(! 'personalInfo' %in% keys.missing)
                missing.firstName = expr('firstName' %in% keys.missing)

                if (  eval(present.personalInfo & missing.firstName) )
                    return (341)
                if (! eval(present.personalInfo & missing.firstName) )
                    return (200)

            }                 ## DONE - LDE Lite
            ldevalidate_structure_lastName <- function (keys.missing) {

                present.personalInfo = expr(! 'personalInfo' %in% keys.missing)
                missing.lastName = expr('lastName' %in% keys.missing)

                if (  eval(present.personalInfo & missing.lastName) )
                    return (341)
                if (! eval(present.personalInfo & missing.lastName) )
                    return (200)

            }                  ## DONE - LDE Lite
            ldevalidate_structure_dateOfBirth <- function (keys.missing) {

                present.personalInfo = expr(! 'personalInfo' %in% keys.missing)
                missing.dateOfBirth = expr('dateOfBirth' %in% keys.missing)

                if (  eval(present.personalInfo & missing.dateOfBirth) )
                    return (341)
                if (! eval(present.personalInfo & missing.dateOfBirth) )
                    return (200)

            }               ## DONE - LDE Lite
            ldevalidate_structure_address <- function (keys.missing) {

                missing.address = expr('address' %in% keys.missing)

                if (  eval(missing.address))
                    return (342)
                if (! eval(missing.address))
                    return (200)

            }                   ## DONE - LDE Lite
            ldevalidate_structure_streetAddress <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Lite
            ldevalidate_structure_city <- function (keys.missing) {
                return (200)
            }                      ## DONE - LDE Lite
            ldevalidate_structure_zip <- function (keys.missing) {
                return (200)
            }                       ## DONE - LDE Lite
            ldevalidate_structure_countryCode <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Lite
            ldevalidate_structure_mobilePhone <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Lite
            ldevalidate_structure_homePhone <- function (keys.missing) {
                return (200)
            }                 ## DONE - LDE Lite
            ldevalidate_structure_bankInfo <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Lite
            ldevalidate_structure_bankName <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Lite
            ldevalidate_structure_abaRoutingNumber <- function (keys.missing) {
                return (200)
            }          ## DONE - LDE Lite
            ldevalidate_structure_accountNumber <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Lite
            ldevalidate_structure_accountType <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Lite
            ldevalidate_structure_accountLength <- function (keys.missing) {
                return (200)
            }             ## DONE - LDE Lite
            ldevalidate_structure_incomeInfo <- function (keys.missing) {
                    return (200)
            }                ## DONE - LDE Lite
            ldevalidate_structure_incomeType <- function (keys.missing) {
                return (200)
            }                ## DONE - LDE Lite
            ldevalidate_structure_payrollType <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Lite
            ldevalidate_structure_payrollFrequency <- function (keys.missing) {
                return (200)
            }          ## DONE - LDE Lite
            ldevalidate_structure_lastPayrollDate <- function (keys.missing) {
                return (200)
            }           ## DONE - LDE Lite
            ldevalidate_structure_nextPayrollDate <- function (keys.missing) {
                return (200)
            }           ## DONE - LDE Lite
            ldevalidate_structure_employmentInfo <- function (keys.missing) {
                    return (200)
            }            ## DONE - LDE Lite
            ldevalidate_structure_employerName <- function (keys.missing) {
                return (200)
            }              ## DONE - LDE Lite
            ldevalidate_structure_hireDate <- function (keys.missing) {
                return (200)
            }                  ## DONE - LDE Lite
            ldevalidate_structure_requestedLoanAmount <- function (keys.missing) {
                return (200)
            }       ## DONE - LDE Lite
            ldevalidate_structure_loanPurpose <- function (keys.missing) {
                return (200)
            }               ## DONE - LDE Lite
            
        }
        
        ldevalidate_map_set = function () {
            list(
                isProduction = ldevalidate_structure_isProduction,
                leadOfferId = ldevalidate_structure_leadOfferId,
                clickId = ldevalidate_structure_clickId,
                campaignId = ldevalidate_structure_campaignId,
                language = ldevalidate_structure_language,
                socialSecurityNumber = ldevalidate_structure_socialSecurityNumber,
                email = ldevalidate_structure_email,
                stateCode = ldevalidate_structure_stateCode,
                grossMonthlyIncome = ldevalidate_structure_grossMonthlyIncome,
                currency = ldevalidate_structure_currency,
                currencyCode = ldevalidate_structure_currencyCode,
                personalInfo = ldevalidate_structure_personalInfo,
                firstName = ldevalidate_structure_firstName,
                lastName = ldevalidate_structure_lastName,
                dateOfBirth = ldevalidate_structure_dateOfBirth,
                address = ldevalidate_structure_address,
                streetAddress = ldevalidate_structure_streetAddress,
                city = ldevalidate_structure_city,
                zip = ldevalidate_structure_zip,
                countryCode = ldevalidate_structure_countryCode,
                mobilePhone = ldevalidate_structure_mobilePhone,
                homePhone = ldevalidate_structure_homePhone,
                bankInfo = ldevalidate_structure_bankInfo,
                bankName = ldevalidate_structure_bankName,
                abaRoutingNumber = ldevalidate_structure_abaRoutingNumber,
                accountNumber = ldevalidate_structure_accountNumber,
                accountType = ldevalidate_structure_accountType,
                accountLength = ldevalidate_structure_accountLength,
                incomeInfo = ldevalidate_structure_incomeInfo,
                incomeType = ldevalidate_structure_incomeType,
                payrollType = ldevalidate_structure_payrollType,
                payrollFrequency = ldevalidate_structure_payrollFrequency,
                lastPayrollDate = ldevalidate_structure_lastPayrollDate,
                nextPayrollDate = ldevalidate_structure_nextPayrollDate,
                employmentInfo = ldevalidate_structure_employmentInfo,
                employerName = ldevalidate_structure_employerName,
                hireDate = ldevalidate_structure_hireDate,
                requestedLoanAmount = ldevalidate_structure_requestedLoanAmount,
                loanPurpose = ldevalidate_structure_loanPurpose
            )
        }
        
        return (ldevalidate_map_set())
        
    }
    
    data.frame(
        keys.missing = keys.missing,
        stringsAsFactors = FALSE
    ) %>% 
        mutate(
            key.codes = keys.missing %>%
                map(
                    .f = function (x) {
                        validator(admethod)[[x]](keys.missing)
                    }
                )
        )
    
    
#     return( validator(admethod)[[key]](value) )
    
}

validateValues = function (key, value, admethod, regex.map, partner.map, ...) {
    
    args = list(...)
    validator = function (admethod) {
        
        if ( partner.map %>% filter(name == admethod) %>% .$lde_type %in% c('Affiliate') ) {
            
            ###  AFFILIATE  ###
            ldevalidate_value_isProduction <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.boolean))

                if (! eval(format.correct))
                    response.vector %<>% c(900)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                ## DONE - LDE Affl
            ldevalidate_value_leadOfferId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(901)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_clickId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(901)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                     ## DONE - LDE Affl
            ldevalidate_value_campaignId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(901)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                  ## DONE - LDE Affl
            ldevalidate_value_language <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(902)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                    ## DONE - LDE Affl
            ldevalidate_value_socialSecurityNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.ssn))
                valid.first3 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(\\d{3})-?(?:\\d{2})-?(?:\\d{4})\\\"$'
                            )
                        ) %in% c('666', '000')
                )
                valid.second3 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:\\d{3})-?(\\d{2})-?(?:\\d{4})\\\"$'
                            )
                        ) %in% c('0')
                )
                valid.last4 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:\\d{3})-?(?:\\d{2})-?(\\d{4})\\\"$'
                            )
                        ) %in% c('0000')
                )




                if (! eval(format.correct) |
                    ! eval(valid.first3) |
                    ! eval(valid.second3) |
                    ! eval(valid.last4) )
                    response.vector %<>% c(315)

                if (  eval(format.correct) &
                      eval(valid.first3) &
                      eval(valid.second3) &
                      eval(valid.last4) )
                    response.vector %<>% c(200)


                return (response.vector)

}        ## DONE - LDE Affl
            ldevalidate_value_email <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.email))
                email.not.mil = expr(
                    ! value %>% 
                        str_detect(
                            pattern = regex(
                                '^\\\".*@.*\\.mil\\\"$'
                            )
                        )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(313)
                if (! eval(email.not.mil))
                    response.vector %<>% c(313)
                if (  eval(format.correct) &
                      eval(email.not.mil) )
                    response.vector %<>% c(200)


                return (response.vector)

}                       ## DONE - LDE Affl
            ldevalidate_value_stateCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.state))
                valid.state = expr(
                    value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\\\"$'
                            )
                        )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(905)
                if (! eval(valid.state))
                    response.vector %<>% c(402)
                if (  eval(format.correct) &
                      eval(valid.state))
                    response.vector %<>% c(200)


                return (response.vector)

}                   ## DONE - LDE Affl
            ldevalidate_value_grossMonthlyIncome <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.float.positive))

                if (! eval(format.correct))
                    response.vector %<>% c(333)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}          ## DONE - LDE Affl
            ldevalidate_value_currency <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.currency))

                if (! eval(format.correct))
                    response.vector %<>% c(907)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                    ## DONE - LDE Affl
            ldevalidate_value_currencyCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.currencyCode))

                if (! eval(format.correct))
                    response.vector %<>% c(907)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                ## DONE - LDE Affl
            ldevalidate_value_firstName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(908)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                   ## DONE - LDE Affl
            ldevalidate_value_lastName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(909)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                    ## DONE - LDE Affl
            ldevalidate_value_dateOfBirth <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (! eval(format.correct) |
                    ! eval(length.correct) )
                    response.vector %<>% c(319)

                if (  eval(format.correct) &
                      eval(length.correct) &
                    ! eval(date.in.past) )
                    response.vector %<>% c(624)

                if (  eval(format.correct) &
                      eval(length.correct) &
                      eval(date.in.past) )
                    response.vector %<>% c(200)



                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_streetAddress <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))
                not.pobox = expr(
                    !value %>% str_detect(
                        pattern = regex(
                            "(?i)^\\\".*p[.]?o[.]?[\\s]*box.*\\\"$"
                        )
                    )
                )
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(911)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (! eval(not.pobox))
                    response.vector %<>% c(608)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}               ## DONE - LDE Affl
            ldevalidate_value_city <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(912)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                        ## DONE - LDE Affl
            ldevalidate_value_zip <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.zip))
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(913)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                         ## DONE - LDE Affl
            ldevalidate_value_countryCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.countryCode))

                if (! eval(format.correct))
                    response.vector %<>% c(403)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_mobilePhone <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.phone))

                if (! eval(format.correct))
                    response.vector %<>% c(701)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_homePhone <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.phone))

                if (! eval(format.correct))
                    response.vector %<>% c(915)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                   ## DONE - LDE Affl
            ldevalidate_value_bankName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(916)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                    ## DONE - LDE Affl
            ldevalidate_value_abaRoutingNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.aba))
                checksum.pass = expr(
                    (
                        value %>% str_remove_all("\"") %>% substr(1,1) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(2,2) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(3,3) %>% as.integer() * 1 +
                        value %>% str_remove_all("\"") %>% substr(4,4) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(5,5) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(6,6) %>% as.integer() * 1 +
                        value %>% str_remove_all("\"") %>% substr(7,7) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(8,8) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(9,9) %>% as.integer() * 1
                    ) %% 10 == 0
                )

                if (! eval(format.correct))
                    response.vector %<>% c(711)
                if (  eval(format.correct) &
                    ! eval(checksum.pass) )
                    response.vector %<>% c(999)
                if (  eval(format.correct) &
                      eval(checksum.pass) )
                    response.vector %<>% c(200)


                return (response.vector)

}            ## DONE - LDE Affl
            ldevalidate_value_accountNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.accountNumber))

                if (! eval(format.correct))
                    response.vector %<>% c(710)
                if (  eval(format.correct) )
                    response.vector %<>% c(200)


                return (response.vector)

}               ## DONE - LDE Affl
            ldevalidate_value_accountType <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.integer))

                if (! eval(format.correct))
                    response.vector %<>% c(712)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_accountLength <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.integer))

                if (! eval(format.correct))
                    response.vector %<>% c(713)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}               ## DONE - LDE Affl
            ldevalidate_value_incomeType <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(921)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                  ## DONE - LDE Affl
            ldevalidate_value_payrollType <- function (value) {

                response.vector = c()

                format.correct = expr(
                    value %>% str_detect(
                        pattern = regex(
                            '^\\\"(?:[Cc]ash|[Cc]heck|[Dd]irect\\s?[Dd]eposit)\\\"$'
                        )
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(705)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            ldevalidate_value_payrollFrequency <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.integer))

                if (! eval(format.correct))
                    response.vector %<>% c(706)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}            ## DONE - LDE Affl
            ldevalidate_value_lastPayrollDate <- function (value) {

                # can be unquoted null OR (date format AND in the past)
                response.vector = c()

                format.isnull = expr(value %>% str_detect(regex.map$unquoted.null))
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (  ! eval(format.isnull) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(707)

                if ( ! eval(format.isnull) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.past) )
                    response.vector %<>% c(707)

                if ( eval(format.isnull) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.past) )
                    response.vector %<>% c(200)


                return (response.vector)

}             ## DONE - LDE Affl
            ldevalidate_value_nextPayrollDate <- function (value) {

                # can be unquoted null OR (date format AND in the future)
                response.vector = c()

                format.isnull = expr(value %>% str_detect(regex.map$unquoted.null))
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.future = expr(
                    value %>% str_remove_all("\"") %>% as.integer() > args$present.time
                )

                if (  ! eval(format.isnull) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(708)

                if ( ! eval(format.isnull) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.future) )
                    response.vector %<>% c(708)

                if ( eval(format.isnull) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.future) )
                    response.vector %<>% c(200)


                return (response.vector)

}             ## DONE - LDE Affl
            ldevalidate_value_employerName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))
                different.from.bankName = expr(
                    value != args$bankName || length(args$bankName) == 0
                )

                if (! eval(format.correct))
                    response.vector %<>% c(926)
                if (! eval(different.from.bankName))
                    response.vector %<>% c(610)
                if (  eval(format.correct) &
                      eval(different.from.bankName) )
                    response.vector %<>% c(200)


                return (response.vector)

}                ## DONE - LDE Affl
            ldevalidate_value_hireDate <- function (value) {

                # can be unquoted null OR (date format AND in the past)
                response.vector = c()

                format.isnull = expr(value %>% str_detect(regex.map$unquoted.null))
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (  ! eval(format.isnull) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(319)

                if ( ! eval(format.isnull) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.past) )
                    response.vector %<>% c(319)

                if ( eval(format.isnull) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.past) )
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Affl
            ldevalidate_value_requestedLoanAmount <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$optional.quoted.float.positive))

                if (! eval(format.correct))
                    response.vector %<>% c(714)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }         ## DONE - LDE Affl
            ldevalidate_value_loanPurpose <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(929)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

}                 ## DONE - LDE Affl
            
            
        } else {
            
            ###  LDE LITE, LDE NON-AFFILIATE  ###
            ldevalidate_value_isProduction <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.boolean))

                if (! eval(format.correct))
                    response.vector %<>% c(900)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                ## DONE - LDE Lite
            ldevalidate_value_leadOfferId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(317)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_clickId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(901)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                     ## DONE - LDE Lite
            ldevalidate_value_campaignId <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(901)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                  ## DONE - LDE Affl
            ldevalidate_value_language <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(902)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Lite
            ldevalidate_value_socialSecurityNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.ssn))
                valid.first3 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(\\d{3})-?(?:\\d{2})-?(?:\\d{4})\\\"$'
                            )
                        ) %in% c('666', '000')
                )
                valid.second3 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:\\d{3})-?(\\d{2})-?(?:\\d{4})\\\"$'
                            )
                        ) %in% c('0')
                )
                valid.last4 = expr(
                    ! value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:\\d{3})-?(?:\\d{2})-?(\\d{4})\\\"$'
                            )
                        ) %in% c('0000')
                )




                if (! eval(format.correct) |
                    ! eval(valid.first3) |
                    ! eval(valid.second3) |
                    ! eval(valid.last4) )
                    response.vector %<>% c(315)

                if (  eval(format.correct) &
                      eval(valid.first3) &
                      eval(valid.second3) &
                      eval(valid.last4) )
                    response.vector %<>% c(200)


                return (response.vector)

            }        ## DONE - LDE Lite
            ldevalidate_value_email <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.email))
                email.not.mil = expr(
                    ! value %>% 
                        str_detect(
                            pattern = regex(
                                '^\\\".*@.*\\.mil\\\"$'
                            )
                        )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(313)
                if (! eval(email.not.mil))
                    response.vector %<>% c(313)
                if (  eval(format.correct) &
                      eval(email.not.mil) )
                    response.vector %<>% c(200)


                return (response.vector)

            }                       ## DONE - LDE Lite
            ldevalidate_value_stateCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.state))
                valid.state = expr(
                    value %>%
                        str_detect(
                            pattern = regex(
                                '^\\\"(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\\\"$'
                            )
                        )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(311)
                if (! eval(valid.state))
                    response.vector %<>% c(402)
                if (  eval(format.correct) &
                      eval(valid.state))
                    response.vector %<>% c(200)


                return (response.vector)

            }                   ## DONE - LDE Lite
            ldevalidate_value_grossMonthlyIncome <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.float.positive))

                if (! eval(format.correct))
                    response.vector %<>% c(332)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }          ## DONE - LDE Lite
            ldevalidate_value_currency <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.currency))

                if (! eval(format.correct))
                    response.vector %<>% c(907)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Lite
            ldevalidate_value_currencyCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.currencyCode))

                if (! eval(format.correct))
                    response.vector %<>% c(907)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                ## DONE - LDE Lite
            ldevalidate_value_firstName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(341)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                   ## DONE - LDE Lite
            ldevalidate_value_lastName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(341)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Lite
            ldevalidate_value_dateOfBirth <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (! eval(format.correct) |
                    ! eval(length.correct) )
                    response.vector %<>% c(319)

                if (  eval(format.correct) &
                      eval(length.correct) &
                    ! eval(date.in.past) )
                    response.vector %<>% c(624)

                if (  eval(format.correct) &
                      eval(length.correct) &
                      eval(date.in.past) )
                    response.vector %<>% c(200)



                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_streetAddress <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))
                not.pobox = expr(
                    !value %>% str_detect(
                        pattern = regex(
                            "(?i)^\\\".*p[.]?o[.]?[\\s]*box.*\\\"$"
                        )
                    )
                )
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(911)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (! eval(not.pobox))
                    response.vector %<>% c(608)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }               ## DONE - LDE Lite
            ldevalidate_value_city <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(912)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                        ## DONE - LDE Lite
            ldevalidate_value_zip <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.zip))
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$unquoted.null) |
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(913)
                if (! eval(not.blank))
                    response.vector %<>% c(342)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                         ## DONE - LDE Lite
            ldevalidate_value_countryCode <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.countryCode))

                if (! eval(format.correct))
                    response.vector %<>% c(403)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_mobilePhone <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.phone))

                if (! eval(format.correct))
                    response.vector %<>% c(915)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_homePhone <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.phone))

                if (! eval(format.correct))
                    response.vector %<>% c(915)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                   ## DONE - LDE Lite
            ldevalidate_value_bankName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(916)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Lite
            ldevalidate_value_abaRoutingNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.aba))
                checksum.pass = expr(
                    (
                        value %>% str_remove_all("\"") %>% substr(1,1) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(2,2) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(3,3) %>% as.integer() * 1 +
                        value %>% str_remove_all("\"") %>% substr(4,4) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(5,5) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(6,6) %>% as.integer() * 1 +
                        value %>% str_remove_all("\"") %>% substr(7,7) %>% as.integer() * 3 +
                        value %>% str_remove_all("\"") %>% substr(8,8) %>% as.integer() * 7 +
                        value %>% str_remove_all("\"") %>% substr(9,9) %>% as.integer() * 1
                    ) %% 10 == 0
                )

                if (! eval(format.correct))
                    response.vector %<>% c(917)
                if (  eval(format.correct) &
                    ! eval(checksum.pass) )
                    response.vector %<>% c(999)
                if (  eval(format.correct) &
                      eval(checksum.pass) )
                    response.vector %<>% c(200)


                return (response.vector)

            }            ## DONE - LDE Lite
            ldevalidate_value_accountNumber <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.accountNumber))
                is.null = expr(
                    value %>% str_detect(regex.map$unquoted.null)
                )
                not.blank = expr(
                    !(
                        value %>% str_detect(regex.map$quoted.blank) |
                        value %>% str_detect(regex.map$unquoted.boolean)
                    )
                )

                if (! eval(is.null) & ! eval(format.correct))
                    response.vector %<>% c(605)
                if (! eval(is.null) & ! eval(not.blank) )
                    response.vector %<>% c(322)
                if (    eval(is.null) |
                     (! eval(is.null) & eval(format.correct) & eval(not.blank) ) )
                    response.vector %<>% c(200)


                return (response.vector)

            }               ## DONE - LDE Lite
            ldevalidate_value_accountType <- function (value) {

                response.vector = c()

                format.correct = expr(
                    value %>% str_detect(
                        regex.map$unquoted.integer
                    ) |
                    value %>% str_detect(
                        regex.map$unquoted.null
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(919)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_accountLength <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$unquoted.integer))

                if (! eval(format.correct))
                    response.vector %<>% c(920)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }               ## DONE - LDE Lite
            ldevalidate_value_incomeType <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(921)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                  ## DONE - LDE Lite
            ldevalidate_value_payrollType <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.name))

                if (! eval(format.correct))
                    response.vector %<>% c(922)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            ldevalidate_value_payrollFrequency <- function (value) {

                response.vector = c()

                format.correct = expr(
                    value %>% str_detect(
                        regex.map$unquoted.integer
                    ) |
                    value %>% str_detect(
                        regex.map$unquoted.null
                    )
                )

                if (! eval(format.correct))
                    response.vector %<>% c(923)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }            ## DONE - LDE Lite
            ldevalidate_value_lastPayrollDate <- function (value) {

                # can be unquoted null OR (date format AND in the past)
                response.vector = c()

                format.isempty = expr(
                    value %>% str_detect(regex.map$unquoted.null) |
                    value %>% str_detect(regex.map$quoted.empty)
                )
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (  ! eval(format.isempty) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(319)

                if ( ! eval(format.isempty) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.past) )
                    response.vector %<>% c(319)

                if ( eval(format.isempty) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.past) )
                    response.vector %<>% c(200)


                return (response.vector)

            }             ## DONE - LDE Lite
            ldevalidate_value_nextPayrollDate <- function (value) {

                # can be unquoted null OR (date format AND in the future)
                response.vector = c()

                format.isempty = expr(
                    value %>% str_detect(regex.map$unquoted.null) |
                    value %>% str_detect(regex.map$quoted.empty)
                )
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.future = expr(
                    value %>% str_remove_all("\"") %>% as.integer() > args$present.time
                )

                if (  ! eval(format.isempty) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(319)

                if ( ! eval(format.isempty) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.future) )
                    response.vector %<>% c(319)

                if ( eval(format.isempty) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.future) )
                    response.vector %<>% c(200)


                return (response.vector)

            }             ## DONE - LDE Lite
            ldevalidate_value_employerName <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))
                different.from.bankName = expr(
                    value != args$bankName || length(args$bankName) == 0
                )

                if (! eval(format.correct))
                    response.vector %<>% c(926)
                if (! eval(different.from.bankName))
                    response.vector %<>% c(610)
                if (  eval(format.correct) &
                      eval(different.from.bankName) )
                    response.vector %<>% c(200)


                return (response.vector)

            }                ## DONE - LDE Lite
            ldevalidate_value_hireDate <- function (value) {

                # can be unquoted null OR (date format AND in the past)
                response.vector = c()

                format.isnull = expr(value %>% str_detect(regex.map$unquoted.null))
                format.correct = expr(value %>% str_detect(regex.map$quoted.date))
                length.correct = expr(value %>% str_remove_all("\"") %>% as.integer() %>% nchar() == 8)
                date.in.past = expr(
                    value %>% str_remove_all("\"") %>% as.integer() <= args$present.time
                )

                if (  ! eval(format.isnull) &
                     (! eval(format.correct) | ! eval(length.correct))  )
                    response.vector %<>% c(319)

                if ( ! eval(format.isnull) &
                      (eval(format.correct) & eval(length.correct)) & ! eval(date.in.past) )
                    response.vector %<>% c(319)

                if ( eval(format.isnull) |
                    (eval(format.correct) & eval(length.correct)) & eval(date.in.past) )
                    response.vector %<>% c(200)


                return (response.vector)

            }                    ## DONE - LDE Lite
            ldevalidate_value_requestedLoanAmount <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$optional.quoted.float.positive))

                if (! eval(format.correct))
                    response.vector %<>% c(928)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }         ## DONE - LDE Lite
            ldevalidate_value_loanPurpose <- function (value) {

                response.vector = c()

                format.correct = expr(value %>% str_detect(regex.map$quoted.string))

                if (! eval(format.correct))
                    response.vector %<>% c(929)
                if (  eval(format.correct))
                    response.vector %<>% c(200)


                return (response.vector)

            }                 ## DONE - LDE Lite
            
        }
        
        ldevalidate_map_set = function () {
            list(
                isProduction = ldevalidate_value_isProduction,
                leadOfferId = ldevalidate_value_leadOfferId,
                click_id = ldevalidate_value_clickId,
                campaign_id = ldevalidate_value_campaignId,
                language = ldevalidate_value_language,
                socialSecurityNumber = ldevalidate_value_socialSecurityNumber,
                email = ldevalidate_value_email,
                stateCode = ldevalidate_value_stateCode,
                grossMonthlyIncome = ldevalidate_value_grossMonthlyIncome,
                currency = ldevalidate_value_currency,
                currencyCode = ldevalidate_value_currencyCode,
                firstName = ldevalidate_value_firstName,
                lastName = ldevalidate_value_lastName,
                dateOfBirth = ldevalidate_value_dateOfBirth,
                streetAddress = ldevalidate_value_streetAddress,
                city = ldevalidate_value_city,
                zip = ldevalidate_value_zip,
                countryCode = ldevalidate_value_countryCode,
                mobilePhone = ldevalidate_value_mobilePhone,
                homePhone = ldevalidate_value_homePhone,
                bankName = ldevalidate_value_bankName,
                abaRoutingNumber = ldevalidate_value_abaRoutingNumber,
                accountNumber = ldevalidate_value_accountNumber,
                accountType = ldevalidate_value_accountType,
                accountLength = ldevalidate_value_accountLength,
                incomeType = ldevalidate_value_incomeType,
                payrollType = ldevalidate_value_payrollType,
                payrollFrequency = ldevalidate_value_payrollFrequency,
                lastPayrollDate = ldevalidate_value_lastPayrollDate,
                nextPayrollDate = ldevalidate_value_nextPayrollDate,
                employerName = ldevalidate_value_employerName,
                hireDate = ldevalidate_value_hireDate,
                requestedLoanAmount = ldevalidate_value_requestedLoanAmount,
                loanPurpose = ldevalidate_value_loanPurpose
            )
        }
        
        return (ldevalidate_map_set())
        
    }
    
    
    return( validator(admethod)[[key]](value) )
    
}

validateRanges = function (key, value, regex.map, ...) {
    
    args = list(...)
    
    if (key %in% regex.map$expected.value.ranges$key) {
        value %in% (regex.map$expected.value.ranges %>% filter(key == key) %>% .$value %>% .[[1]])
    } else {
        TRUE
    }
    
}

getEvaluationDF = function (test.payloads) {

#     setGlobalVars()
    regex.map = setEvaluationVariables()
    partner.map = setPartnerMappingLDE()
    
    
    test.payloads %>%
        mutate(
            ###  Source Details  ###
            control.file = admethod %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                partner.map %>% filter(name == x) %>% .$control_file
                            },
                            error = function (e) {
                                return(9001)
                            }
                        )
                    }
                ),
            
            lde.version = admethod %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                partner.map %>% filter(name == x) %>% .$lde_type
                            },
                            error = function (e) {
                                return(9002)
                            }
                        )
                    }
                ),
            

            ###  DF Components  ###
            objects = raw_lead %>%
                str_match_all(
                    pattern = regex(regex.map$keys)
                ) %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                x %>% .[ ,2] %>%
                                    unlist() %>%
                                    as.data.frame(stringsAsFactors = FALSE) %>%
                                    filter(
                                        . %in% regex.map$expected.objects
                                    ) %>% .$.
                            },
                            error = function (e) {
                                return(9003)
                            }
                        )
                    }
                ),

            
            keys = raw_lead %>%
                str_match_all(
                    pattern = regex(regex.map$keys)
                ) %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                x %>% .[ ,2] %>%
                                    unlist() %>%
                                    as.data.frame(stringsAsFactors = FALSE) %>%
                                    filter(
                                        !. %in% regex.map$expected.objects
                                    ) %>% .$.
                            },
                            error = function (e) {
                                return(9004)
                            }
                        )
                    }
                ),
            
            
            values = raw_lead %>%
                str_match_all(
                    pattern = regex(regex.map$values)
                ) %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                x %>% 
                                    as.data.frame(
                                        stringsAsFactors = FALSE
                                    ) %>% 
                                    mutate(
                                        matches = coalesce(V2, V3)
                                    ) %>% .$matches
                            },
                            error = function (e) {
                                return(9005)
                            }
                        )
                    }
                ),

            
            ###  Create the DF  ###
            validate.values = 
                map2(
                
                    ####  Combine Key Value Pairs  ####
                    .x = keys,
                    .y = values,
                    .f = function (x,y) {
                        tryCatch(
                            expr = {
                                data.frame(
                                    key = x,
                                    value = y,
                                    stringsAsFactors = FALSE
                                )
                            },
                            error = function (e) {
                                return(data.frame(value.codes = 9006))
                            }
                        )
                    }
                
                ),
            
                ####  Valid Value = Value Value Validation  ####
            validate.values = pmap(
                .l = list(validate.values, lead_time, admethod),
                .f = function (a,b,c) {
                    
                    tryCatch(
                        expr = {

                            present.time = paste0(
                                year(b),
                                month(b) %>% str_pad(width = 2, side = "left", pad = '0'),
                                day(b) %>% str_pad(width = 2, side = "left", pad = '0')
                            )

                            bankName = a %>% filter(key == 'bankName') %>% .$value

                            a %>% 

                                mutate(
                                    ######  Error Code - Raw_Request_Validator.clj  ######
                                    value.codes = pmap(
                                        .l = list(key, value),
                                        .f = function (a, b) {
                                            tryCatch(
                                                expr = {
                                                    validateValues(
                                                        key = a,
                                                        value = b,
                                                        admethod = c,
                                                        present.time = present.time,
                                                        bankName = bankName,
                                                        regex.map = regex.map,
                                                        partner.map = partner.map
                                                    )
                                                },
                                                error = function (e) {
                                                    return(9000)
                                                }
                                            )
                                        }
                                    ),
                                    
                                    ######  Value Ranges  ######
                                    value.in.range = pmap(
                                        .l = list(key, value),
                                        .f = function (a, b) {
                                            tryCatch(
                                                expr = {
                                                    validateRanges(
                                                        key = a,
                                                        value = b,
                                                        regex.map = regex.map
                                                    )
                                                },
                                                error = function (e) {
                                                    return(NA)
                                                }
                                            )
                                        }
                                    )
                                )
                        }, 
                        error = function (e) {
                            return(data.frame(value.codes = 9007))
                        }
                    )
                }
            ),



            ##  Keys Comparison  ##
            keys.missing = validate.values %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                regex.map$expected.keys[ which(!regex.map$expected.keys %in% unlist(x %>% .$key)) ]
                            },
                            error = function (e) {
                                return(9008)
                            }
                        )
                    }
                ),

            keys.extra = validate.values %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                unlist(x %>% .$key)[ which(!unlist(x %>% .$key) %in% regex.map$expected.keys) ]
                            },
                            error = function (e) {
                                return(9009)
                            }
                        )
                    }
                ),
            
            validate.keys =
                pmap(
                    .l = list(admethod, keys.missing),
                    .f = function (a, b) {
                        tryCatch(
                            expr = {
                                validateStructure(
                                    admethod = a,
                                    keys.missing = b,
                                    partner.map = partner.map
                                )
                            },
                            error = function (e) {
                                return(data.frame(key.codes = 8000))
                            }
                        )
                    }
                ),



            ##  Objects Comparison  ##
            objects.missing = objects %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                regex.map$expected.objects[ which(! regex.map$expected.objects %in% unlist(x)) ]
                            },
                            error = function (e) {
                                return(9010)
                            }
                        )
                    }
                ),

            objects.extra = objects %>%
                map(
                    .f = function (x) {
                        tryCatch(
                            expr = {
                                x[ which(! x %in% regex.map$expected.objects) ]
                            },
                            error = function (e) {
                                return(9011)
                            }
                        )
                    }
                ),
            
            validate.objects =
                pmap(
                    .l = list(admethod, objects.missing),
                    .f = function (a, b) {
                        tryCatch(
                            expr = {
                                validateStructure(
                                    admethod = a,
                                    keys.missing = b,
                                    partner.map = partner.map
                                )
                            },
                            error = function (e) {
                                return(data.frame(key.codes = 8000))
                            }
                        )
                    }
                )
        )
    
}

finalEvaluation = function (admethod, sample.size) {
    
    payloads = admethod %>% getPayloads(limit = sample.size)
    evaluation = payloads %>% getEvaluationDF()

    evaluation %>% 
        mutate(
            lde.error.count =
                pmap(
                    .l = list(validate.values, validate.keys, validate.objects),
                    .f = function (a, b, c) {
                        a$value.codes %>% map(function (x) {min(x) != 200 & min(x) < 900}) %>% unlist() %>% sum() +
                        b$key.codes %>% map(function (x) {min(x) != 200 & min(x) < 900}) %>% unlist() %>% sum() +
                        c$key.codes %>% map(function (x) {min(x) != 200 & min(x) < 900}) %>% unlist() %>% sum() 
                    }
                ) %>% unlist(),
            ops.error.count =
                pmap(
                    .l = list(validate.values, validate.keys, validate.objects),
                    .f = function (a, b, c) {
                        a$value.codes %>% map(function (x) {min(x) >= 900}) %>% unlist() %>% sum() +
                        b$key.codes %>% map(function (x) {min(x) >= 900}) %>% unlist() %>% sum() +
                        c$key.codes %>% map(function (x) {min(x) >= 900}) %>% unlist() %>% sum() 
                    }
                ) %>% unlist()
#         ) %>%
#         group_by(
#             lde.error = lde.error.count > 0,
#             ops.error = ops.error.count > 0
#         ) %>% 
#         summarize(
#             n = n()
        )

}

# getAdmethodList() %>% head(10)

payloadErrorRegCount = function (payload, reg) {
    
    payload %>%
        left_join(
            reg,
            by = 'email'
        ) %>%
        summarize(
            n(),
            n_distinct(lead_id),
            n_distinct(name)
        )
}

payloadErrorFields = function (payload, reg) {
    
    joined = payload %>%
        left_join(
            reg,
            by = 'email'
        )

    has.reg = joined %>%
        filter(
            !is.na(email)
        ) %>%
        transmute(
            name,
            validate.values = validate.values %>%
                map(
                    .f = function(x) {
                        if (ncol(x) > 1) {
                            x %>% filter( value.codes %>% map(max) != 200 )
                        }
                    }
                ),
            fields
        )

    expand.grid(
        payload.key = has.reg$validate.values %>% map(function (x) {x$key}) %>% unlist(),
        input.field = has.reg$fields %>% .[[1]],
        stringsAsFactors = FALSE
    ) %>% 
    group_by(
        payload.key,
        input.field
    ) %>% 
    summarize(
        count = n()
    ) %>% 
    ungroup() %>% 
    arrange(
        payload.key
    )
    
}

payloadErrorSummary = function (payload) {
    
    do.call(
        what = rbind,
        args = payload %>%
                    filter(
                        !is.na(name)
                    ) %>% 
                    .$validate.values %>%
                    map(
                        .f = function(x) {
                            if (ncol(x) > 1) {
                                x %>% filter( value.codes %>% map(max) != 200 )
                            }
                        }
                    )
    ) %>%
    group_by(
        key,
        value.codes = value.codes %>% map(max) %>% unlist()
    ) %>%
    summarize(
        count = n()
    ) %>% 
    ungroup() %>% 
    arrange(
        desc(count)
    )
    
}

regFieldsSummary = function (payload, reg) {
    
    do.call(
        c,
        payload %>%
            left_join(
                reg,
                by = 'email'
            ) %>%
            filter(
                !is.na(email)
            ) %>% .$fields
    ) %>%
    as.data.frame(
        stringsAsFactors = FALSE
    ) %>%
    select(
        inputs = '.'
    ) %>%
    group_by(
        inputs
    ) %>%
    summarize(
        count = n()
    ) %>% 
    ungroup() %>% 
    arrange(
        desc(count)
    )
    
}

# #####  Test Cases  #####

# evaluation %>% slice(1) %>% .$json.df %>% .[[1]] %>% select(key, value) %>%
# mutate(
#     codes = pmap(
#         .l = list(key, value),
#         .f = function (a, b) {
#             try({
#                 validateValues(
#                     key = a,
#                     value = b,
#                     admethod = 'LenderEdge 4',
#                     present.time = '\"20191216\"',
#                     bankName = ''
#                 )
#             })
#         }
#     )
# )

# validateValues(
#     key = 'isProduction',
#     value = 'true',
#     admethod = 'LenderEdge 4'
# ) == c(200)

# validateValues(
#     key = 'stateCode',
#     value = 'AA',
#     admethod = 'LenderEdge 4'
# ) == c(905, 402)

# validateValues(
#     key = 'email',
#     value = '\"tobywilkes0@gmail.com\"',
#     admethod = 'LenderEdge 4'
# ) == c(200)

# validateValues(
#     key = 'abaRoutingNumber',
#     value = '\"071214579\"', 
#     admethod = 'Aff FixMedia 4'
# ) == c(200)

# # evaluation %>% nrow()
# slice.num = 106
# leadgroup.evaluation %>% slice(slice.num) %>% .$validate.values %>% .[[1]]
# leadgroup.evaluation %>% slice(slice.num) %>% .$validate.keys %>% .[[1]]
# leadgroup.evaluation %>% slice(slice.num) %>% .$validate.objects %>% .[[1]]
# leadgroup.evaluation %>% slice(slice.num)
# leadgroup.evaluation %>% slice(slice.num) %>% .$raw_lead %>% cat()
# leadgroup.evaluation %>% slice(slice.num) %>% .$raw_lead %>% prettify()
