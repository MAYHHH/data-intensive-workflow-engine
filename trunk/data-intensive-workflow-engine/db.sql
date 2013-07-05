-- phpMyAdmin SQL Dump
-- version 4.0.4
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Jun 27, 2013 at 01:51 PM
-- Server version: 5.5.29-0ubuntu0.12.04.1
-- PHP Version: 5.3.10-1ubuntu3.5

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `workflow_engine`
--
CREATE DATABASE IF NOT EXISTS `workflow_engine` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;
USE `workflow_engine`;

-- --------------------------------------------------------

--
-- Table structure for table `exec_site`
--

CREATE TABLE IF NOT EXISTS `exec_site` (
  `esid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`esid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `file`
--

CREATE TABLE IF NOT EXISTS `file` (
  `fid` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `estsize` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `schedule`
--

CREATE TABLE IF NOT EXISTS `schedule` (
  `sid` int(11) NOT NULL AUTO_INCREMENT,
  `tid` int(11) NOT NULL,
  `wkid` int(11) NOT NULL,
  PRIMARY KEY (`sid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `worker`
--

CREATE TABLE IF NOT EXISTS `worker` (
  `wkid` int(11) NOT NULL AUTO_INCREMENT,
  `hostname` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `cpu` float NOT NULL,
  `total_memory` float NOT NULL,
  `free_memory` float NOT NULL,
  `total_space` float NOT NULL,
  `free_space` float NOT NULL,
  `updated` int(11) NOT NULL,
  `esid` int(11) NOT NULL,
  `unit_cost` int(11) NOT NULL,
  PRIMARY KEY (`wkid`),
  UNIQUE KEY `hostname` (`hostname`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=4 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow`
--

CREATE TABLE IF NOT EXISTS `workflow` (
  `wfid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `updated` int(11) NOT NULL,
  `status` char(1) NOT NULL,
  `start` int(11) NOT NULL DEFAULT '-1',
  `finish` int(11) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`wfid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow_task`
--

CREATE TABLE IF NOT EXISTS `workflow_task` (
  `tid` int(11) NOT NULL AUTO_INCREMENT,
  `wfid` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `cmd` varchar(255) NOT NULL,
  `args` varchar(255) NOT NULL COMMENT 'argument string seperated by ";"',
  `status` char(1) NOT NULL COMMENT '''W''aiting, ''E''xecuting, ''C''ompleted',
  `estopr` int(11) NOT NULL COMMENT 'Estimated number of atomic operations',
  `start` int(11) NOT NULL DEFAULT '-1',
  `finish` int(11) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`tid`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow_task_depen`
--

CREATE TABLE IF NOT EXISTS `workflow_task_depen` (
  `wtdid` int(11) NOT NULL AUTO_INCREMENT,
  `parent` int(11) NOT NULL,
  `child` int(11) NOT NULL,
  `wfid` int(11) NOT NULL,
  PRIMARY KEY (`wtdid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow_task_file`
--

CREATE TABLE IF NOT EXISTS `workflow_task_file` (
  `wtfid` int(11) NOT NULL AUTO_INCREMENT,
  `type` char(1) NOT NULL,
  `tid` int(11) NOT NULL,
  `fid` int(11) NOT NULL,
  PRIMARY KEY (`wtfid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;
