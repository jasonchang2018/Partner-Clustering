(ns leadzeppelin.contextualizer.raw-request-validator
  (:require [audit.client :as audit]
            [clojure.string :as str]
            [leadzeppelin.contextualizer.core :refer [Router proceed terminate]]
            [leadzeppelin.malformed-input-detector :as mid]
            [leadzeppelin.persistence.config :as db-cfg]
            [leadzeppelin.persistence.core :as db :refer [with-db-conn]]
            [taoensso.timbre :as log]))

(defn ^:private normalize [s]
  (-> s
      (str/replace #"\W" "")
      str/lower-case))

(defn strings-match? [x y]
  ;; Compares two strings after normalizing them
  ;; if both of them are not nils and not empty.
  (let [x (and x (normalize x))
        y (and y (normalize y))]
    (and (seq x) (seq y) (= x y))))

(
    def lde-lite-validation-rules
    [
        ( 
            fn [{:keys [stateCode]}]
            (
                cond 
                    (empty? stateCode) 311
                    (not (mid/state-code-valid? stateCode)) 402
            )
        )
        (
            fn [{:keys [socialSecurityNumber]}]
            (
                cond
                    (empty? socialSecurityNumber) 314
                    (not (mid/ssn-valid? socialSecurityNumber)) 315
            )
        )
        (
            fn [{:keys [leadOfferId]}]
            (
                when
                    (empty? leadOfferId) 317
            )
        )
        (
            fn [{:keys [grossMonthlyIncome]}]
            (
                when
                    (empty? (str grossMonthlyIncome)) 332
            )
        )
        (
            fn [{:keys [grossMonthlyIncome]}]
            (
                when-not
                    (mid/number-or-number-string? grossMonthlyIncome) 333
            )
        )
        (
            fn [{:keys [email]}]
            (
                cond
                    (empty? email) 312
                    (not (mid/email-valid? email)) 313
                    (clojure.string/ends-with? email ".mil") 606
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                    (empty? bankInfo) nil
                    (nil? (:accountNumber bankInfo)) nil
                    (str/blank? (:accountNumber bankInfo)) 322
                    (not (mid/account-number-valid? (:accountNumber bankInfo))) 605
                )
            )
        (
            fn [{:keys [personalInfo]}]
            (
                when 
                    (empty? personalInfo) 341
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                when 
                    (empty? (get-in personalInfo [:dateOfBirth])) 341
            )
        )
        (
            fn [{{dob :dateOfBirth} :personalInfo}]
            (
                cond
                    (not (mid/date-of-birth-valid? dob)) 319
                    (not (mid/date-in-past? dob)) 624
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                when
                    (empty? (get-in personalInfo [:firstName])) 341
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                when
                    (empty? (get-in personalInfo [:lastName])) 341
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                let [
                    {:keys [streetAddress city zip] :as address}
                    (get-in personalInfo [:address])
                ]
                (
                when
                    (or 
                        (empty? address)
                        (str/blank? streetAddress)
                        (str/blank? city)
                        (str/blank? zip)
                    ) 342
                )
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                cond
                    (nil? (get-in personalInfo [:address :countryCode])) nil
                    (not (
                        contains? #{"US" "USA"}
                        (get-in personalInfo [:address :countryCode])
                    )) 403
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                let [street-address (get-in personalInfo [:address :streetAddress])]
                (
                    cond
                    (nil? street-address) nil
                    (mid/po-box? street-address) 608
                )
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                    (empty? incomeInfo) nil
                    (empty? (:lastPayrollDate incomeInfo)) nil
                    (not (mid/last-payroll-date-valid? (:lastPayrollDate incomeInfo))) 319
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                (empty? incomeInfo) nil
                (nil? (:nextPayrollDate incomeInfo)) nil
                (not (mid/next-payroll-date-valid? (:nextPayrollDate incomeInfo))) 319
            )
        )
        (
            fn [{:keys [employmentInfo]}]
            (
                cond
                (empty? employmentInfo) nil
                (not (mid/hire-date-valid? (:hireDate employmentInfo))) 319
            )
        )
        (
            fn [{:keys [employmentInfo bankInfo]}]
            (
                cond
                (empty? employmentInfo) nil
                (strings-match?
                    (:bankName bankInfo)
                    (:employerName employmentInfo)
                ) 610
            )
        )
    ]
)

    ;; TODO: We are still missing error code 710 (for account number) due
    ;; to inconsistencies in the LDE4 documentation.
(
    def lde-affiliate-validation-rules
    [
        (
            fn [{:keys [requestedLoanAmount]}]
            (
                when
                    (or
                        (not (int? requestedLoanAmount))
                        (<= requestedLoanAmount 0)
                    ) 714
            )
        )
        (
            fn [{:keys [personalInfo]}]
            (
                cond
                    (empty? personalInfo) nil
                    (empty? (:mobilePhone personalInfo)) 701
            )
        )
        (
            fn [{:keys [employmentInfo]}]
            (
                when
                    (empty? employmentInfo) 702
            )
        )
        (
            fn [{:keys [employmentInfo]}]
            (
                cond
                    (empty? employmentInfo) nil
                    (empty? (:employerName employmentInfo)) 702
                )
            )
        (
            fn [{:keys [employmentInfo]}]
            (
                cond
                    (empty? employmentInfo) nil
                    (empty? (:hireDate employmentInfo)) 703
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                when
                    (empty? incomeInfo) 704
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                    (empty? incomeInfo) nil
                    (empty? (:incomeType incomeInfo)) 704
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                    (empty? incomeInfo) nil
                    (empty? (:payrollType incomeInfo)) 705
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                (empty? incomeInfo) nil
                (nil? (:payrollFrequency incomeInfo)) 706
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                (empty? incomeInfo) nil
                (str/blank? (:lastPayrollDate incomeInfo)) 707
            )
        )
        (
            fn [{:keys [incomeInfo]}]
            (
                cond
                (empty? incomeInfo) nil
                (nil? (:nextPayrollDate incomeInfo)) 708
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                when
                    (empty? bankInfo) 709
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                    (empty? bankInfo) nil
                    (empty? (:bankName bankInfo)) 709
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                (empty? bankInfo) nil
                (nil? (:accountNumber bankInfo)) 710
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                (empty? bankInfo) nil
                (not (mid/routing-number-valid? (:abaRoutingNumber bankInfo))) 711
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                (empty? bankInfo) nil
                (nil? (:accountType bankInfo)) 712
            )
        )
        (
            fn [{:keys [bankInfo]}]
            (
                cond
                (empty? bankInfo) nil
                (nil? (:accountLength bankInfo)) 713
            )
        )
    ]
)

(def lead-type->validation-rules
  {:lite lde-lite-validation-rules
   :non-affiliate lde-lite-validation-rules
   :affiliate (concat lde-lite-validation-rules
                      lde-affiliate-validation-rules)})

(defn to-str [data] (when-not (nil? data) (str data)))

(defn- coerce-to-integer
  "
  (coerce-to-integer 0) ;;=> 0
  (coerce-to-integer \"10\") ;;=> 10
  (coerce-to-integer \"10.5\") ;;=> 11
  (coerce-to-integer \"i'm not a number\") ;;=> throws
  (coerce-to-integer 3.14) ;;=> 3
  "
  [x]
  (cond (integer? x) x
        (double? x) (Math/round x)
        :else (Math/round (Double/parseDouble x))))

(defn coerce-request [request]
  (update request :grossMonthlyIncome coerce-to-integer))

(defn validate-lead
  [rule-set router {:keys [request OriginationID] :as body}]
  (if-let [[error-code :as result]
           (seq (filter some? ((apply juxt rule-set) request)))]
    (do (log/info "Validation error codes:" (pr-str result))
        (terminate router body error-code))
    (let [coerced-request (coerce-request request)]
      (db/save-coerced-request!
       (db-cfg/persistence) OriginationID coerced-request)
      (proceed router (assoc body
                             :raw-request request
                             :request coerced-request
                             :status nil)))))

(defn validator
  [router body]
  (let [rule-set (lead-type->validation-rules (keyword (:PartnerType body)))]
    (if rule-set
      (validate-lead rule-set router body)
      (throw (Exception. (str "There is no ruleset for Partner Type "
                              (:PartnerType body) "!"))))))