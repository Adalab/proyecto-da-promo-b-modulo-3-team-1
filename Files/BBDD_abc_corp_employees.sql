-- -----------------------------------------------------
-- Schema ABC_CORP_EMPLOYEES
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema ABC_CORP_EMPLOYEES
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `ABC_CORP_EMPLOYEES` DEFAULT CHARACTER SET utf8 ;
USE `ABC_CORP_EMPLOYEES` ;

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`PersonalDetails` 
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`PersonalDetails` (
  `EmployeeNumber` INT NOT NULL AUTO_INCREMENT,
  `Age` INT NULL DEFAULT NULL,
  `Education` INT NOT NULL,
  `EducationField` VARCHAR(45) NULL DEFAULT NULL,
  `Gender` VARCHAR(45) NULL DEFAULT NULL,
  `MaritalStatus` VARCHAR(45) NULL DEFAULT NULL,
  `DateBirth` INT NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`)
);


-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`CompensationDetails`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`CompensationDetails` (
  `EmployeeNumber` INT NOT NULL,
  `MonthlyIncome` INT NULL DEFAULT NULL,            
  `PercentSalaryHike` INT NULL DEFAULT NULL,        
  `StockOptionLevel` INT NULL DEFAULT NULL,          
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_CompensationDetails_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
);

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`JobDetails`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`JobDetails` (
  `EmployeeNumber` INT NOT NULL,
  `MonthlyRate` INT NULL DEFAULT NULL,
  `Department` VARCHAR(45) NULL DEFAULT NULL,
  `JobLevel` INT NULL DEFAULT NULL,
  `JobRole` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_JobDetails_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
);

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`Satisfaction`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`Satisfaction` (
  `EmployeeNumber` INT NOT NULL,
  `EnvironmentSatisfaction` INT NULL DEFAULT NULL,  -- Corregido el nombre
  `RelationshipSatisfaction` INT NULL DEFAULT NULL,
  `JobSatisfaction` INT NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_Satisfaction_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
);

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`WorkPersonalDetails`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`WorkPersonalDetails` (
  `EmployeeNumber` INT NOT NULL,
  `NumCompaniesWorked` INT NULL DEFAULT NULL,
  `TotalWorkingYears` INT NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_WorkPersonalDetails_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
    );

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`YearsAtCompany`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`YearsAtCompany` (
  `EmployeeNumber` INT NOT NULL,
  `YearsAtCompany` INT NULL DEFAULT NULL,
  `YearsSinceLastPromotion` INT NULL DEFAULT NULL,
  `YearsWithCurrManager` INT NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_YearsAtCompany_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
    );

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`WorkDetails`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`WorkDetails` (
  `EmployeeNumber` INT NOT NULL,
  `DistanceFromHome` INT NULL DEFAULT NULL,
  `BusinessTravel` VARCHAR(45) NULL DEFAULT NULL,
  `OverTime` VARCHAR(45) NULL DEFAULT NULL,
  `TrainingTimesLastYear` INT NULL DEFAULT NULL,
  `WorkLifeBalance` INT NULL DEFAULT NULL,
  `RemoteWork` VARCHAR(45) NULL DEFAULT NULL,
  `Attrition` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_WorkDetails_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
);

-- -----------------------------------------------------
-- Table `ABC_CORP_EMPLOYEES`.`PerformanceDetails`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `ABC_CORP_EMPLOYEES`.`PerformanceDetails` (
  `EmployeeNumber` INT NOT NULL,
  `PerformanceRating` INT NULL DEFAULT NULL,
  `JobInvolvement` INT NULL DEFAULT NULL,
  PRIMARY KEY (`EmployeeNumber`),
  CONSTRAINT `fk_PerformanceDetails_PersonalDetails`
    FOREIGN KEY (`EmployeeNumber`)
    REFERENCES `ABC_CORP_EMPLOYEES`.`PersonalDetails` (`EmployeeNumber`)
);
