const fs = require('fs');
const path = require('path');

const sourceDir = path.join(__dirname, '../node_modules/@topcoder/finance-prisma-client/packages/finance-prisma-client');
const targetDir = path.join(__dirname, '../node_modules/@topcoder/finance-prisma-client');

// Check if source exists
if (!fs.existsSync(sourceDir)) {
  console.warn('Warning: Finance Prisma Client package not found at', sourceDir);
  console.warn('This is expected if the package is not yet installed. Skipping setup.');
  process.exit(0);
}

// Copy package.json, dist, prisma, and README.md to target
const filesToCopy = ['package.json', 'dist', 'prisma', 'README.md'];

filesToCopy.forEach(file => {
  const source = path.join(sourceDir, file);
  const target = path.join(targetDir, file);
  
  if (fs.existsSync(source)) {
    if (fs.statSync(source).isDirectory()) {
      // Remove existing directory if it exists
      if (fs.existsSync(target)) {
        fs.rmSync(target, { recursive: true, force: true });
      }
      // Copy directory recursively
      copyRecursiveSync(source, target);
    } else {
      // Copy file
      fs.copyFileSync(source, target);
    }
    console.log(`Copied ${file} to @topcoder/finance-prisma-client`);
  }
});

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

console.log('Finance Prisma Client setup complete!');
