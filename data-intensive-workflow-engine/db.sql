-- phpMyAdmin SQL Dump
-- version 4.0.4
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Jul 09, 2013 at 09:43 AM
-- Server version: 5.5.29-0ubuntu0.12.04.1
-- PHP Version: 5.3.10-1ubuntu3.5

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `workflow_engine`
--
CREATE DATABASE IF NOT EXISTS `workflow_engine` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `workflow_engine`;

-- --------------------------------------------------------

--
-- Table structure for table `exec_site`
--

CREATE TABLE IF NOT EXISTS `exec_site` (
  `esid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`esid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `file`
--

CREATE TABLE IF NOT EXISTS `file` (
  `fid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `estsize` double NOT NULL DEFAULT '0',
  PRIMARY KEY (`fid`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1132 ;

-- --------------------------------------------------------

--
-- Table structure for table `schedule`
--

CREATE TABLE IF NOT EXISTS `schedule` (
  `sid` int(11) NOT NULL AUTO_INCREMENT,
  `tid` int(11) NOT NULL,
  `wkid` int(11) NOT NULL,
  PRIMARY KEY (`sid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `worker`
--

CREATE TABLE IF NOT EXISTS `worker` (
  `wkid` int(11) NOT NULL AUTO_INCREMENT,
  `hostname` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `cpu` float DEFAULT '0',
  `total_memory` float DEFAULT '0',
  `free_memory` float DEFAULT '0',
  `total_space` float DEFAULT '0',
  `free_space` float DEFAULT '0',
  `updated` int(11) DEFAULT '0',
  `esid` int(11) DEFAULT '0',
  `unit_cost` int(11) DEFAULT '0',
  `current_tid` int(11) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`wkid`),
  UNIQUE KEY `hostname` (`hostname`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=55 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow`
--

CREATE TABLE IF NOT EXISTS `workflow` (
  `wfid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `submitted` int(11) NOT NULL,
  `status` char(1) NOT NULL,
  `start` int(11) NOT NULL DEFAULT '-1',
  `finish` int(11) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`wfid`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=75 ;

-- --------------------------------------------------------

--
-- Table structure for table `workflow_task`
--

CREATE TABLE IF NOT EXISTS `workflow_task` (
  `tid` int(11) NOT NULL AUTO_INCREMENT,
  `wfid` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `cmd` text NOT NULL,
  `status` char(1) NOT NULL COMMENT '''W''aiting, ''E''xecuting, ''C''ompleted',
  `estopr` int(11) NOT NULL COMMENT 'Estimated number of atomic operations',
  `start` int(11) NOT NULL DEFAULT '-1',
  `finish` int(11) NOT NULL DEFAULT '-1',
  `exit_value` int(11) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`tid`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

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
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Stand-in structure for view `_completed_parent_task`
--
CREATE TABLE IF NOT EXISTS `_completed_parent_task` (
`tid` int(11)
);
-- --------------------------------------------------------

--
-- Stand-in structure for view `_completed_parent_task_count`
--
CREATE TABLE IF NOT EXISTS `_completed_parent_task_count` (
`tid` int(11)
,`parents` bigint(21)
);
-- --------------------------------------------------------

--
-- Stand-in structure for view `_parent_task_count`
--
CREATE TABLE IF NOT EXISTS `_parent_task_count` (
`tid` int(11)
,`parents` bigint(21)
);
-- --------------------------------------------------------

--
-- Stand-in structure for view `_task_to_dispatch`
--
CREATE TABLE IF NOT EXISTS `_task_to_dispatch` (
`tid` int(11)
,`hostname` varchar(255)
);
-- --------------------------------------------------------

--
-- Stand-in structure for view `_workflow_input_file`
--
CREATE TABLE IF NOT EXISTS `_workflow_input_file` (
`wfid` int(11)
,`tid` int(11)
,`fid` int(11)
,`name` varchar(255)
,`estsize` double
);
-- --------------------------------------------------------

--
-- Table structure for table `_workflow_task_remain_parents`
--
-- in use(#1356 - View 'workflow_engine._workflow_task_remain_parents' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them)

-- --------------------------------------------------------

--
-- Structure for view `_completed_parent_task`
--
DROP TABLE IF EXISTS `_completed_parent_task`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `_completed_parent_task` AS select `workflow_task`.`tid` AS `tid` from `workflow_task` where (`workflow_task`.`status` = 'C');

-- --------------------------------------------------------

--
-- Structure for view `_completed_parent_task_count`
--
DROP TABLE IF EXISTS `_completed_parent_task_count`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `_completed_parent_task_count` AS select `t`.`tid` AS `tid`,count(`d`.`parent`) AS `parents` from ((`workflow_task` `t` left join `workflow_task_depen` `d` on((`t`.`tid` = `d`.`child`))) join `workflow_task` `p` on((`d`.`parent` = `p`.`tid`))) where (`p`.`status` = 'C') group by `t`.`tid`;

-- --------------------------------------------------------

--
-- Structure for view `_parent_task_count`
--
DROP TABLE IF EXISTS `_parent_task_count`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `_parent_task_count` AS select `t`.`tid` AS `tid`,count(`d`.`parent`) AS `parents` from (`workflow_task` `t` left join `workflow_task_depen` `d` on((`t`.`tid` = `d`.`child`))) group by `t`.`tid`;

-- --------------------------------------------------------

--
-- Structure for view `_task_to_dispatch`
--
DROP TABLE IF EXISTS `_task_to_dispatch`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `_task_to_dispatch` AS select min(`t`.`tid`) AS `tid`,`wk`.`hostname` AS `hostname` from ((((`_parent_task_count` `pc` left join `_completed_parent_task_count` `cpc` on((`pc`.`tid` = `cpc`.`tid`))) join `schedule` `s` on((`pc`.`tid` = `s`.`tid`))) join `worker` `wk` on((`s`.`wkid` = `wk`.`wkid`))) join `workflow_task` `t` on((`t`.`tid` = `pc`.`tid`))) where (((`pc`.`parents` - ifnull(`cpc`.`parents`,0)) = 0) and (`wk`.`current_tid` = -(1)) and (`t`.`status` <> 'C')) group by `wk`.`hostname`;

-- --------------------------------------------------------

--
-- Structure for view `_workflow_input_file`
--
DROP TABLE IF EXISTS `_workflow_input_file`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `_workflow_input_file` AS select `t`.`wfid` AS `wfid`,`i`.`tid` AS `tid`,`f`.`fid` AS `fid`,`f`.`name` AS `name`,`f`.`estsize` AS `estsize` from (((`workflow_task_file` `i` left join `workflow_task_file` `o` on(((`i`.`fid` = `o`.`fid`) and (`i`.`type` <> `o`.`type`)))) join `workflow_task` `t` on((`i`.`tid` = `t`.`tid`))) join `file` `f` on((`i`.`fid` = `f`.`fid`))) where ((`i`.`type` = 'I') and isnull(`o`.`tid`));
