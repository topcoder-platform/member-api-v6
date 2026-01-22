const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const sourceDir = path.join(__dirname, '../node_modules/@topcoder/finance-prisma-client/packages/finance-prisma-client');
const targetDir = path.join(__dirname, '../node_modules/@topcoder/finance-prisma-client');
const repoRoot = path.join(__dirname, '../node_modules/@topcoder/finance-prisma-client');

console.log('Setting up Finance Prisma Client...');
console.log('Source directory:', sourceDir);
console.log('Target directory:', targetDir);

if (!fs.existsSync(repoRoot)) {
  console.error('Error: @topcoder/finance-prisma-client package not found at', repoRoot);
  console.error('Make sure the package is installed: yarn install or npm install');
  process.exit(1);
}

if (!fs.existsSync(sourceDir)) {
  console.error('Error: Finance Prisma Client package not found at', sourceDir);
  console.error('Expected structure: node_modules/@topcoder/finance-prisma-client/packages/finance-prisma-client');
  console.error('Actual structure in repo root:');
  try {
    const repoContents = fs.readdirSync(repoRoot);
    console.error('  Contents:', repoContents.join(', '));
  } catch (err) {
    console.error('  Could not read repo root:', err.message);
  }
  process.exit(1);
}

const sourcePackageJson = path.join(sourceDir, 'package.json');
if (!fs.existsSync(sourcePackageJson)) {
  console.error('Error: package.json not found in', sourceDir);
  process.exit(1);
}

const distDir = path.join(sourceDir, 'dist');
if (!fs.existsSync(distDir)) {
  console.warn('Warning: dist directory not found. Attempting to build the package...');
  try {
    const dbUrl = process.env.FINANCE_DATABASE_URL || process.env.DATABASE_URL || 'postgresql://dummy:dummy@localhost:5432/dummy';
    
    console.log('Installing package dependencies (including devDependencies)...');
    execSync('npm install --include=dev', { 
      cwd: sourceDir, 
      stdio: 'inherit',
      env: { ...process.env, DATABASE_URL: dbUrl }
    });
    
    console.log('Building package...');
    execSync('npm run build', { 
      cwd: sourceDir, 
      stdio: 'inherit',
      env: { ...process.env, DATABASE_URL: dbUrl }
    });
    console.log('Build completed successfully');
  } catch (err) {
    console.error('Error building package:', err.message);
    console.error('Please ensure the package is built before installation, or run: npm run build in the package directory');
    process.exit(1);
  }
}

if (!fs.existsSync(targetDir)) {
  fs.mkdirSync(targetDir, { recursive: true });
}

const filesToCopy = ['package.json', 'dist', 'prisma', 'README.md'];

let copiedCount = 0;
filesToCopy.forEach(file => {
  const source = path.join(sourceDir, file);
  const target = path.join(targetDir, file);
  
  if (fs.existsSync(source)) {
    try {
      if (fs.statSync(source).isDirectory()) {
        if (fs.existsSync(target)) {
          fs.rmSync(target, { recursive: true, force: true });
        }
        copyRecursiveSync(source, target);
      } else {
        fs.copyFileSync(source, target);
      }
      console.log(`Copied ${file} to @topcoder/finance-prisma-client`);
      copiedCount++;
    } catch (err) {
      console.error(`Error copying ${file}:`, err.message);
      process.exit(1);
    }
  } else {
    console.warn(`Warning: ${file} not found in source directory`);
  }
});

// Verify that dist/index.js exists after copying
const distIndex = path.join(targetDir, 'dist', 'index.js');
if (!fs.existsSync(distIndex)) {
  console.error('Error: dist/index.js not found after setup. Expected at:', distIndex);
  console.error('This file is required for the package to work.');
  process.exit(1);
}

console.log(`Finance Prisma Client setup complete! Copied ${copiedCount} items.`);

function copyRecursiveSync(src, dest) {
  const exists = fs.existsSync(src);
  const stats = exists && fs.statSync(src);
  const isDirectory = exists && stats.isDirectory();
  
  if (isDirectory) {
    if (!fs.existsSync(dest)) {
      fs.mkdirSync(dest, { recursive: true });
    }
    fs.readdirSync(src).forEach(childItemName => {
      copyRecursiveSync(
        path.join(src, childItemName),
        path.join(dest, childItemName)
      );
    });
  } else {
    fs.copyFileSync(src, dest);
  }
}
