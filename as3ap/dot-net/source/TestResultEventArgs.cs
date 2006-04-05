//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2003-2006  Carlos Guzman Alvarez
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lesser.html
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//

using System;

namespace AS3AP.BenchMark
{
    public class TestResultEventArgs : EventArgs
    {
        #region Fields

        private string testName = String.Empty;
        private object testResult;
        private TimeSpan testTime;
        private bool testFailed = false;

        #endregion

        #region Properties

        public string TestName
        {
            get { return this.testName; }
            set { this.testName = value; }
        }

        public object TestResult
        {
            get { return this.testResult; }
            set { this.testResult = value; }
        }

        public TimeSpan TestTime
        {
            get { return this.testTime; }
            set { this.testTime = value; }
        }

        public bool TestFailed
        {
            get { return this.testFailed; }
            set { this.testFailed = value; }
        }

        #endregion

        #region Constructors

        public TestResultEventArgs(string testName, object testResult, TimeSpan testTime, bool testFailed)
        {
            this.testName = testName;
            if (testResult == null)
            {
                testResult = 0;
            }
            this.testResult = testResult;
            this.testTime = testTime;
            this.testFailed = testFailed;
        }

        #endregion
    }
}
