-- Data Warehouse WishHack
-- dom 17 jun 2018 02:03:28 -03
-- Model: New Model    Version: 2.0
-- Created by Laercio Serra (laercio.serra@gmail.com)

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `mydb` ;

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `mydb` DEFAULT CHARACTER SET utf8 ;
USE `mydb` ;

-- -----------------------------------------------------
-- Table `mydb`.`FactSales`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`FactSales` ;

CREATE TABLE IF NOT EXISTS `mydb`.`FactSales` (
  `Transaction_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `Customer_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `Product_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `Merchant_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `TransactionID` VARCHAR(50) NOT NULL,
  `TransactionDate` DATETIME NULL,
  `IsGift` VARCHAR(5) NULL,
  `IsNewRefund` VARCHAR(5) NULL,
  `PaymentID` VARCHAR(50) NULL,
  `PaymentName` VARCHAR(5) NULL,
  `PaymentType` VARCHAR(5) NULL,
  `PaymentTypeNumber` VARCHAR(5) NULL,
  `Subtotal` DECIMAL(4,2) NULL,
  `Taxes` DECIMAL(4,2) NULL,
  `Total` DECIMAL(4,2) NULL,
  PRIMARY KEY (`Transaction_SK`),
  UNIQUE INDEX `idCustomer_UNIQUE` (`Transaction_SK` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`DimCustomer`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`DimCustomer` ;

CREATE TABLE IF NOT EXISTS `mydb`.`DimCustomer` (
  `Customer_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `UserID` VARCHAR(50) NOT NULL,
  `FullName` VARCHAR(100) NOT NULL COMMENT '	',
  `ShortName` VARCHAR(50) NOT NULL,
  `Email` VARCHAR(50) NOT NULL,
  `City` VARCHAR(45) CHARACTER SET 'big5' NULL,
  `Country` VARCHAR(45) NULL,
  `CountryCode` VARCHAR(5) NULL,
  `PhoneNumber` VARCHAR(15) NULL,
  `State` VARCHAR(45) NULL,
  `StateAbbreviation` VARCHAR(5) NULL,
  `StreetAddress1` VARCHAR(45) NULL,
  `StreetAddress2` VARCHAR(45) NULL,
  `Zipcode` VARCHAR(10) NULL,
  `HasProfilePic` VARCHAR(5) NULL,
  `IsWishStar` VARCHAR(5) NULL,
  `IsFollowing` VARCHAR(5) NULL,
  `IsFacebookUser` VARCHAR(5) NULL,
  `IsTemporary` VARCHAR(5) NULL,
  PRIMARY KEY (`Customer_SK`),
  UNIQUE INDEX `idCustomer_UNIQUE` (`Customer_SK` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`ProductTagAttr`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`ProductTagAttr` ;

CREATE TABLE IF NOT EXISTS `mydb`.`ProductTagAttr` (
  `ProductID` VARCHAR(50) NOT NULL,
  `ProductTag` VARCHAR(100) NOT NULL COMMENT '\n',
  PRIMARY KEY (`ProductID`),
  UNIQUE INDEX `idCustomer_UNIQUE` (`ProductID` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`ProductReviewAttr`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`ProductReviewAttr` ;

CREATE TABLE IF NOT EXISTS `mydb`.`ProductReviewAttr` (
  `ReviewID` VARCHAR(50) NOT NULL COMMENT '	',
  `ProductID` VARCHAR(50) NOT NULL,
  `Rating` VARCHAR(5) NULL,
  `Date` DATETIME NULL,
  `UpvoteCount` VARCHAR(5) NULL,
  `UserID` VARCHAR(50) NOT NULL,
  `TransactionID` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`ReviewID`))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`DimProduct`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`DimProduct` ;

CREATE TABLE IF NOT EXISTS `mydb`.`DimProduct` (
  `Product_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `ProductID` VARCHAR(50) NOT NULL,
  `ManufacturerID` VARCHAR(50) NULL,
  `MaxDeliveryTime` DATETIME NULL,
  `MaxFulfillmentTime` DATETIME NULL,
  `MaxShipTime` DATETIME NULL,
  `MinDeliveryTime` DATETIME NULL,
  `MinFulfillmentTime` DATETIME NULL,
  `MinShipTime` DATETIME NULL,
  `Price` DECIMAL(4,2) NULL,
  `IsC2c` VARCHAR(5) NULL,
  `IsFbw` VARCHAR(5) NULL,
  `IsFreeGift` VARCHAR(5) NULL,
  `IsFreeSample` VARCHAR(5) NULL,
  `IsHiddenMysteryBoxItem` VARCHAR(5) NULL,
  `IsMysteryBox` VARCHAR(5) NULL,
  `IsWishExpress` VARCHAR(5) NULL,
  PRIMARY KEY (`Product_SK`, `ProductID`),
  UNIQUE INDEX `idCustomer_UNIQUE` (`Product_SK` ASC),
  INDEX `fk_DimProduct_ProductTagAttr_idx` (`ProductID` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`DimMerchant`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`DimMerchant` ;

CREATE TABLE IF NOT EXISTS `mydb`.`DimMerchant` (
  `Merchant_SK` INT NOT NULL COMMENT 'Surrogate Key',
  `MerchantID` VARCHAR(50) NOT NULL COMMENT '\n',
  `MerchantFullName` VARCHAR(250) NOT NULL,
  `MerchantShortName` VARCHAR(45) NOT NULL,
  `MerchantDisplayName` VARCHAR(45) NOT NULL,
  `IsStrategic` VARCHAR(5) NULL,
  `IsWishOwned` VARCHAR(5) NULL,
  PRIMARY KEY (`Merchant_SK`, `MerchantID`),
  UNIQUE INDEX `idCustomer_UNIQUE` (`Merchant_SK` ASC),
  INDEX `fk_DimStore_FactSales1_idx` (`MerchantID` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`WishListDetailAttr`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`WishListDetailAttr` ;

CREATE TABLE IF NOT EXISTS `mydb`.`WishListDetailAttr` (
  `ItemID` VARCHAR(50) NOT NULL,
  `ItemName` VARCHAR(100) NOT NULL,
  `SellerPrice` DECIMAL(15,2) NULL,
  `CatID` VARCHAR(50) NOT NULL,
  `ParentCatID` VARCHAR(50) NOT NULL,
  `IsParent` VARCHAR(5) NULL,
  `CategoryName` VARCHAR(50) NULL,
  `CategoryLevel` VARCHAR(50) NULL,
  PRIMARY KEY (`ItemID`))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `mydb`.`WhisListAttr`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `mydb`.`WhisListAttr` ;

CREATE TABLE IF NOT EXISTS `mydb`.`WhisListAttr` (
  `UserID` VARCHAR(50) NOT NULL,
  `ListID` VARCHAR(50) NOT NULL,
  `ListName` VARCHAR(100) NOT NULL,
  `Private` VARCHAR(5) NULL,
  `ItemID` VARCHAR(50) NOT NULL,
  INDEX `fk_CustomerWhisListAttr_DimCustomer1_idx` (`UserID` ASC),
  PRIMARY KEY (`UserID`, `ItemID`),
  INDEX `fk_WhisListAttr_WishListDetailAttr1_idx` (`ItemID` ASC))
ENGINE = MyISAM
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
